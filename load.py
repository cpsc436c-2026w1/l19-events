"""Many simultaneous queries while updates stream in: latency of the queries, and how stale an answer can be.

N simulated users (asyncio tasks) ask the store for the latest price of a product while a stream handler applies
the simulator's price updates to that store at the times the simulator gave them. The clock is the simulator's,
played faster than real time (SPEED). Two numbers come out:

    latency     real time of one query against the local store (p50, p99)
    staleness   for an answer that is no longer the latest price: query time minus the event time (observed_at) of
                the first newer update the answer misses. 0 when the answer is the latest price. (Query time minus
                the event time of the returned price would measure how long ago a price last changed, which is
                long for any product that is not being repriced.)

The requirement: no answer is stale by more than the window (300 s). It holds when the handler keeps up. With
--handler-delay 4 the handler needs 4 s for each update, falls behind during the burst of store north, and the
report says so.

The store is in memory, with the two operations the DynamoDB table would have. What changes on DynamoDB:
    get_latest      GetItem on (product_code, store); eventually consistent by default, ConsistentRead=True for
                    the latest committed write; latency is a network call (milliseconds), not a dict lookup
    put_if_newer    PutItem with ConditionExpression "attribute_not_exists(observed_at) OR observed_at < :new",
                    so an older update that arrives late does not replace a newer price
    the handler     a Lambda function on the stream: its delay is the batch window plus the invocation, and one
                    shard is processed in order, so one slow batch delays every update behind it
    the users       requests from outside; a throttled table or a cold start shows up in p99, not in p50

    uv run python load.py                       # 50 users, the 10 minutes from 09:20 (the burst)
    uv run python load.py --handler-delay 4     # a slow handler: answers older than the window
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel

from inputs import PriceUpdate

WINDOW_S = 300
SPEED = 200                 # simulated seconds per real second
HOUR = datetime(2026, 10, 31, 9, 0, 0, tzinfo=timezone.utc).timestamp()
RECORDS = Path(__file__).parent / "events" / "price-updates" / "records.jsonl"


class LatestPrices:
    """The store the users read: one item per (product_code, store)."""

    def __init__(self):
        self.items: dict[tuple[str, str], dict] = {}

    async def get_latest(self, product_code: str, store: str) -> dict | None:
        await asyncio.sleep(0)                                   # a query yields to the other tasks, as a network call does
        return self.items.get((product_code, store))

    async def put_if_newer(self, u: PriceUpdate) -> bool:
        await asyncio.sleep(0)
        old = self.items.get((u.product_code, u.store))
        if old and old["observed_at"] >= u.observed_at.timestamp():   # the condition of the conditional write
            return False
        self.items[(u.product_code, u.store)] = {"price": u.price, "observed_at": u.observed_at.timestamp(),
                                                 "update_id": u.update_id}
        return True


class LoadReport(BaseModel):
    users: int
    queries: int
    updates_applied: int
    p50_ms: float
    p99_ms: float
    stale_answers: int
    max_staleness_s: float
    max_handler_lag_s: float              # longest time between an update reaching the stream and being applied
    window_s: int = WINDOW_S
    within_window: bool

    def line(self) -> str:
        behind = (f"the handler is behind by up to {self.max_handler_lag_s:.0f} s" if self.max_handler_lag_s > 30
                  else "updates reached the stream late")
        verdict = "within the window" if self.within_window else f"OLDER THAN THE WINDOW: {behind}"
        return (f"{self.users} users, {self.queries} queries, {self.updates_applied} updates applied\n"
                f"latency p50 {self.p50_ms:.3f} ms, p99 {self.p99_ms:.3f} ms (in-memory store)\n"
                f"largest staleness {self.max_staleness_s:.0f} s, window {self.window_s} s: {verdict}")


def load_updates(start_s: int, minutes: int) -> list[PriceUpdate]:
    """The stream's records written in the period, each once, in stream order."""
    seen, out = set(), []
    for line in RECORDS.read_text().splitlines():
        u = PriceUpdate.from_record(json.loads(line))
        if u.event_id not in seen and start_s <= u.created_at - HOUR < start_s + minutes * 60 and u.currency == "CAD":
            seen.add(u.event_id)
            out.append(u)
    return out


async def run(users: int = 50, start_s: int = 20 * 60, minutes: int = 10, handler_delay_s: float = 0.0,
              seed: int = 19, speed: int = SPEED) -> LoadReport:
    updates = load_updates(start_s, minutes)
    store, t0 = LatestPrices(), time.perf_counter()
    now = lambda: start_s + (time.perf_counter() - t0) * speed          # noqa: E731  simulated seconds since 09:00
    end = start_s + minutes * 60
    by_item: dict[tuple[str, str], list[PriceUpdate]] = {}
    for u in updates:
        by_item.setdefault((u.product_code, u.store), []).append(u)
    keys = sorted(by_item)
    latencies, staleness, lags, applied = [], [], [0.0], 0

    async def handler() -> None:
        """Applies the updates in stream order, each when it has reached the stream, one after another."""
        nonlocal applied
        for u in updates:
            wait = (u.created_at - HOUR) - now()
            if wait > 0:
                await asyncio.sleep(wait / speed)
            if handler_delay_s:
                await asyncio.sleep(handler_delay_s / speed)             # the time the handler needs for one update
            applied += await store.put_if_newer(u)
            lags.append(now() - (u.created_at - HOUR))

    async def user(n: int) -> None:
        r = random.Random(f"{seed}:user:{n}")
        while now() < end:
            await asyncio.sleep(r.uniform(0.5, 3.5) / speed)             # think time, in simulated seconds
            key, asked = r.choice(keys), now()
            t = time.perf_counter()
            answer = await store.get_latest(*key)
            latencies.append((time.perf_counter() - t) * 1000)
            have = answer["observed_at"] if answer else 0.0
            missed = [u.observed_at.timestamp() for u in by_item[key]
                      if have < u.observed_at.timestamp() <= HOUR + asked]
            staleness.append(HOUR + asked - min(missed) if missed else 0.0)

    feeder = asyncio.create_task(handler())
    await asyncio.gather(*(user(n) for n in range(users)))
    feeder.cancel()
    lat = sorted(latencies)
    worst = max(staleness)
    return LoadReport(users=users, queries=len(lat), updates_applied=applied, p50_ms=statistics.median(lat),
                      p99_ms=lat[int(len(lat) * 0.99) - 1], stale_answers=sum(s > 0 for s in staleness),
                      max_staleness_s=round(worst, 1), max_handler_lag_s=round(max(lags), 1),
                      within_window=worst <= WINDOW_S)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--users", type=int, default=50)
    ap.add_argument("--start", type=int, default=20, help="minutes after 09:00")
    ap.add_argument("--minutes", type=int, default=10)
    ap.add_argument("--handler-delay", type=float, default=0.0, help="simulated seconds the handler needs per update")
    a = ap.parse_args()
    print(asyncio.run(run(a.users, a.start * 60, a.minutes, a.handler_delay)).line())
