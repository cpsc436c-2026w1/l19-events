"""Change stream of the latest-price table: one count and one average price per store for every 5 minutes.

A Lambda function on a DynamoDB stream with a tumbling window (Lambda developer guide, services-ddb-windows).
Lambda calls `handler` with a batch of records, the window and the state the previous call returned; the last
call for a window has isFinalInvokeForWindow true. The function returns the new state.

Rules, all in code (TRIAGE.md):
    redelivered-update   a record whose eventID is already in the state is skipped
    late-update          Lambda sets the window from the time the record entered the stream; an update whose
                         observed_at is before the window's start becomes a correction for its own window
    eur-update           the table holds CAD only; another currency is rejected, not converted
    store-burst          a store's count above 3 times its median earlier window is flagged
    quiet-windows        `final_numbers` reports a window without stored numbers as a count of 0

    uv run python stream_window.py
"""
from __future__ import annotations

import json
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pydantic import ValidationError

from inputs import PriceUpdate, problems
from results import Correction, Rejected, WindowResult

WINDOW_S = 300
BURST_FACTOR = 3
MIN_HISTORY = 3                    # windows of a store needed before a burst can be flagged
STORES = ("north", "centre", "online")


class Sink:
    """Where the numbers go (a table in AWS). Corrections and rejections are keyed by eventID: a retried
    invocation writes the same row again, not a second one."""

    def __init__(self):
        self.windows: list[WindowResult] = []
        self.corrections: dict[str, Correction] = {}
        self.rejected: dict[str, Rejected] = {}

    def median_count(self, store: str) -> float | None:
        counts = [w.count for w in self.windows if w.store == store]
        return statistics.median(counts) if len(counts) >= MIN_HISTORY else None


SINK = Sink()


def _parse(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def window_start(ts: str) -> str:
    t = _parse(ts)
    return _iso(t - timedelta(seconds=(t.minute * 60 + t.second) % WINDOW_S))


def handler(event: dict, context=None, validate: bool = True, dedupe: bool = True) -> dict:
    """validate=False and dedupe=False exist for two demos: a function that does not check its input raises on a
    bad record (failing_batch.py), and one that does not skip a record it has seen counts it twice (replay.py)."""
    state = event.get("state") or {}
    seen, stores = set(state.get("seen", [])), {k: dict(v) for k, v in state.get("stores", {}).items()}
    start, end = event["window"]["start"], event["window"]["end"]
    for record in event["Records"]:
        if dedupe and record["eventID"] in seen:                     # delivered before: count it once
            continue
        if validate:
            try:
                PriceUpdate.from_record(record)                      # the boundary: input that does not fit stops here
            except ValidationError as e:
                seen.add(record["eventID"])
                SINK.rejected[record["eventID"]] = Rejected(
                    event_id=record["eventID"], store=str(record["dynamodb"].get("Keys", {}).get("store", {}).get("S")),
                    currency="", reason="; ".join(problems(e)))
                continue
        if record["eventName"] == "REMOVE":
            seen.add(record["eventID"])
            continue
        item = record["dynamodb"]["NewImage"]
        store, price = item["store"]["S"], float(item["price"]["N"])     # without validation: KeyError or ValueError
        seen.add(record["eventID"])
        if item["currency"]["S"] != "CAD":
            SINK.rejected[record["eventID"]] = Rejected(event_id=record["eventID"], store=store,
                                                        currency=item["currency"]["S"], reason="the table holds CAD only")
            continue
        if item["observed_at"]["S"] < start:                         # late: it belongs to an earlier window
            SINK.corrections[record["eventID"]] = Correction(
                event_id=record["eventID"], store=store, window_start=window_start(item["observed_at"]["S"]), price=price)
            continue
        s = stores.setdefault(store, {"count": 0, "sum": 0.0})
        s["count"], s["sum"] = s["count"] + 1, round(s["sum"] + price, 2)
    if event.get("isFinalInvokeForWindow"):
        for store in sorted(stores):
            s, usual = stores[store], SINK.median_count(store)
            SINK.windows.append(WindowResult(
                store=store, window_start=start, window_end=end, count=s["count"],
                avg_price=round(s["sum"] / s["count"], 2), burst=usual is not None and s["count"] > BURST_FACTOR * usual))
    return {"state": {"seen": sorted(seen), "stores": stores}, "batchItemFailures": []}


def final_numbers(sink: Sink, first_window: str, windows: int) -> dict[str, dict[str, dict]]:
    """Count and average per window and store with the corrections applied. A window without stored numbers
    gets a count of 0: this is the check on a schedule that reports the quiet windows."""
    out = {}
    for i in range(windows):
        start = _iso(_parse(first_window) + timedelta(seconds=i * WINDOW_S))
        out[start] = {}
        for store in STORES:
            stored = [w for w in sink.windows if (w.store, w.window_start) == (store, start)]
            count = sum(w.count for w in stored)
            total = sum(w.avg_price * w.count for w in stored)
            late = [c for c in sink.corrections.values() if (c.store, c.window_start) == (store, start)]
            count, total = count + len(late), total + sum(c.price for c in late)
            out[start][store] = {"count": count, "avg_price": round(total / count, 2) if count else None}
    return out


def run_local(records: list[dict], batch_size: int = 10, **options) -> None:
    """Call the handler as Lambda does: the records of one window in batches, the state passed on, then a
    final call for the window. The window comes from ApproximateCreationDateTime."""
    by_window: dict[str, list[dict]] = {}
    for r in records:
        created = datetime.fromtimestamp(r["dynamodb"]["ApproximateCreationDateTime"], tz=timezone.utc)
        by_window.setdefault(window_start(_iso(created)), []).append(r)
    for start in sorted(by_window):
        window = {"start": start, "end": _iso(_parse(start) + timedelta(seconds=WINDOW_S))}
        state, batch = {}, by_window[start]
        for i in range(0, len(batch), batch_size):
            state = handler({"Records": batch[i:i + batch_size], "window": window, "state": state,
                             "isFinalInvokeForWindow": False, "isWindowTerminatedEarly": False}, **options)["state"]
        handler({"Records": [], "window": window, "state": state, "isFinalInvokeForWindow": True,
                 "isWindowTerminatedEarly": False}, **options)


if __name__ == "__main__":
    folder = Path(__file__).parent / "events" / "price-updates"
    run_local([json.loads(line) for line in (folder / "records.jsonl").read_text().splitlines()])
    for start, per in final_numbers(SINK, "2026-10-31T09:00:00Z", 12).items():
        flags = [w.store for w in SINK.windows if w.window_start == start and w.burst]
        print(start[11:16], {s: (v["count"], v["avg_price"]) for s, v in per.items()},
              f"burst: {flags}" if flags else "", "quiet" if all(v["count"] == 0 for v in per.values()) else "")
    print(f"{len(SINK.corrections)} late updates corrected, {len(SINK.rejected)} rejected")
