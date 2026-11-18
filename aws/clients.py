"""The three runs against the real tables: two writers at once, a failing batch, many queries.

Every function takes boto3 clients, so the tests run them on moto. `deploy.py` calls them with real clients.
One writer function, `put_update`, writes every item, with or without the condition.
"""
from __future__ import annotations

import json
import random
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

RECORDS = Path(__file__).parent.parent / "events" / "price-updates" / "records.jsonl"


def iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def put_update(ddb, table: str, product_code: str, store: str, price: float | None, observed_at: str, update_id: str,
               if_newer: bool = False) -> bool:
    """Write one price update. With if_newer the write is conditional: it replaces the item only when the item
    is not there or holds an older observed_at (DynamoDB developer guide, "Condition expressions"). Returns False
    when the condition failed. price=None leaves the price out: the planted bad record of the batch run."""
    item = {"product_code": {"S": product_code}, "store": {"S": store}, "currency": {"S": "CAD"},
            "observed_at": {"S": observed_at}, "update_id": {"S": update_id}}
    if price is not None:
        item["price"] = {"N": f"{price:.2f}"}
    extra = {}
    if if_newer:
        extra = {"ConditionExpression": "attribute_not_exists(observed_at) OR observed_at < :new",
                 "ExpressionAttributeValues": {":new": {"S": observed_at}}}
    try:
        ddb.put_item(TableName=table, Item=item, **extra)
        return True
    except ddb.exceptions.ConditionalCheckFailedException:
        return False


def where_am_i(timeout: float = 1.0) -> str:
    """Where this client runs: the EC2 instance type and Availability Zone from the instance metadata service,
    or "not on EC2" (a laptop)."""
    base = "http://169.254.169.254/latest"
    try:
        req = urllib.request.Request(f"{base}/api/token", method="PUT",
                                     headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"})
        token = urllib.request.urlopen(req, timeout=timeout).read().decode()
        get = lambda p: urllib.request.urlopen(urllib.request.Request(   # noqa: E731
            f"{base}/meta-data/{p}", headers={"X-aws-ec2-metadata-token": token}), timeout=timeout).read().decode()
        return f"EC2 {get('instance-type')} in {get('placement/availability-zone')}"
    except Exception:
        return "not on EC2 (a laptop or another network)"


# ---- 1. two writers at once -----------------------------------------------------------------------------------

def run_race(make_client, table: str, run_id: str, rounds: int = 50, stale: int = 10) -> dict:
    """Two clients write the same item. In each round both send an update with the same observed_at at the same
    moment, each with its own price: the condition lets exactly one through. Then each sends `stale` updates that
    are older than the item: none may win."""
    product, store = f"race-{run_id}", "race"
    t0 = datetime(2026, 10, 31, 9, 0, 0, tzinfo=timezone.utc)
    clients = {"A": make_client(), "B": make_client()}
    barrier = threading.Barrier(2)
    won = {"A": 0, "B": 0}
    lost = {"A": 0, "B": 0}
    errors: list[str] = []
    per_round: list[int] = [0] * rounds

    def writer(name: str) -> None:
        for i in range(rounds):
            barrier.wait()
            try:
                ok = put_update(clients[name], table, product, store, 5.00 + (0.01 if name == "B" else 0) + i,
                                iso(t0 + timedelta(seconds=i)), f"{name}-{i:03d}", if_newer=True)
            except Exception as e:                         # anything that is not the failed condition
                errors.append(f"{name} round {i}: {type(e).__name__}")
                continue
            (won if ok else lost)[name] += 1
            per_round[i] += ok
        for i in range(stale):                             # late arrivals: older than what the item holds
            ok = put_update(clients[name], table, product, store, 1.00, iso(t0 - timedelta(seconds=i + 1)),
                            f"{name}-late-{i:02d}", if_newer=True)
            (won if ok else lost)[name] += 1

    threads = [threading.Thread(target=writer, args=(n,)) for n in clients]
    [t.start() for t in threads]
    [t.join() for t in threads]
    item = clients["A"].get_item(TableName=table, Key={"product_code": {"S": product}, "store": {"S": store}},
                                 ConsistentRead=True)["Item"]
    return {"writes": 2 * (rounds + stale), "won": won, "lost": lost, "other_errors": errors,
            "rounds": rounds, "rounds_with_exactly_one_winner": sum(n == 1 for n in per_round),
            "final_item": {"observed_at": item["observed_at"]["S"], "price": item["price"]["N"],
                           "update_id": item["update_id"]["S"]},
            "expected_observed_at": iso(t0 + timedelta(seconds=rounds - 1))}


def race_lines(r: dict) -> list[str]:
    f = r["final_item"]
    return [f"two writers, {r['writes']} conditional writes on one item: {sum(r['won'].values())} won "
            f"(A {r['won']['A']}, B {r['won']['B']}), {sum(r['lost'].values())} lost, {len(r['other_errors'])} other errors",
            f"rounds with exactly one winner: {r['rounds_with_exactly_one_winner']} of {r['rounds']}",
            f"final item: observed_at {f['observed_at']} (the newest sent: {r['expected_observed_at']}), "
            f"price {f['price']}, written by {f['update_id']}"]


# ---- 2. a failing batch ---------------------------------------------------------------------------------------

def write_batch(ddb, table: str, run_id: str, good: int = 20, bad_at: int = 11) -> dict:
    """`good` updates and one without a price, to a store of this run only, so that only this run's rows are read."""
    store, now = f"batch-{run_id}", datetime.now(timezone.utc)
    n = 0
    for i in range(1, good + 2):
        bad = i == bad_at
        if not bad:
            n += 1
        put_update(ddb, table, f"{'bad' if bad else 'good'}-{i:03d}", store, None if bad else 4.00 + n * 0.10,
                   iso(now), f"{run_id}-{i:03d}")
    return {"store": store, "good": good, "bad_position": bad_at, "written_at": iso(now), "epoch": time.time(),
            "expected_sum": round(sum(4.00 + k * 0.10 for k in range(1, good + 1)), 2)}


def read_counts(ddb, windows_table: str, store: str) -> dict:
    """What the function stored for one store: window rows, and late updates stored as corrections."""
    def rows(kind):
        return ddb.query(TableName=windows_table, KeyConditionExpression="pk = :p", ConsistentRead=True,
                         ExpressionAttributeValues={":p": {"S": f"{kind}#{store}"}})["Items"]
    windows, corrections = rows("window"), rows("correction")
    return {"windows": [{"window_start": w["sk"]["S"].split("#")[0], "shard": w["sk"]["S"].split("#")[-1][-4:],
                         "count": int(w["count"]["N"]), "avg_price": float(w["avg_price"]["N"])} for w in windows],
            "counted": sum(int(w["count"]["N"]) for w in windows) + len(corrections),
            "corrections": len(corrections)}


def read_failures(sqs, queue_url: str, wait_s: int = 5) -> list[dict]:
    """The invocation records in the on-failure queue (read and deleted)."""
    out = []
    while True:
        got = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=wait_s).get("Messages", [])
        if not got:
            return out
        for m in got:
            out.append(json.loads(m["Body"]))
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])


def wait_for_batch(ddb, sqs, windows_table: str, queue_url: str, written: dict, deadline_s: int = 660,
                   poll_s: int = 15, sleep=time.sleep, clock=time.time) -> dict:
    """Poll until every good update is counted and the queue has a record, or the deadline passes. The numbers
    appear when the 5-minute window ends, so this takes up to one window and a little more."""
    end, failures, counts = clock() + deadline_s, [], {"counted": 0, "windows": [], "corrections": 0}
    while True:
        failures += read_failures(sqs, queue_url, wait_s=1)
        counts = read_counts(ddb, windows_table, written["store"])
        done = counts["counted"] >= written["good"] and failures
        if done or clock() >= end:
            return {"observed": bool(done), "waited_s": round(deadline_s - (end - clock())), **counts,
                    "failures": failures}
        sleep(poll_s)


def batch_lines(written: dict, got: dict) -> list[str]:
    lines = [f"wrote {written['good']} good updates and 1 without a price (position {written['bad_position']}) "
             f"to store {written['store']}; waited {got['waited_s']} s"]
    if not got["observed"]:
        lines.append(f"NOT OBSERVED before the deadline: {got['counted']} counted, {len(got['failures'])} queue records")
    lines.append(f"window table: {got['counted']} of {written['good']} good updates counted "
                 f"({len(got['windows'])} rows, one per window and shard: "
                 f"{' + '.join(str(w['count']) for w in got['windows']) or '0'}; {got['corrections']} corrections): "
                 f"{'each once' if got['counted'] == written['good'] else 'MISMATCH'}")
    for f in got["failures"]:
        b, c = f.get("DDBStreamBatchInfo", {}), f.get("requestContext", {})
        lines.append(f"on-failure queue: condition {c.get('condition')}, invoke count {c.get('approximateInvokeCount')}, "
                     f"batchSize {b.get('batchSize')}, sequence numbers {b.get('startSequenceNumber')} to "
                     f"{b.get('endSequenceNumber')}")
    return lines


# ---- 3. many queries ------------------------------------------------------------------------------------------

def burst_updates(limit: int = 120) -> list[tuple[str, float]]:
    """(product code, price) of the simulator's burst of store north: the updates the writer replays."""
    out = []
    for line in RECORDS.read_text().splitlines():
        img = json.loads(line)["dynamodb"]["NewImage"]
        if img["store"]["S"] == "north" and "09:20:00" <= img["observed_at"]["S"][11:19] < "09:22:00":
            out.append((img["product_code"]["S"], float(img["price"]["N"])))
    return out[:limit]


def run_load(ddb, table: str, run_id: str, users: int = 50, seconds: float = 30.0, writes_per_s: float = 4.0,
             consistent: bool = False, seed: int = 19) -> dict:
    """`users` threads share one client and read the latest price of random products while one writer applies
    the burst's updates. Latency is the whole GetItem call as the client sees it. Staleness: for an answer that
    is not the newest acknowledged write for its key at the moment the query started, the time since that write
    was acknowledged; both times come from this process's clock."""
    store, updates = f"load-{run_id}", burst_updates()
    keys = sorted({code for code, _ in updates})
    acked: dict[str, list[tuple[float, str]]] = {k: [] for k in keys}      # per key: (time acknowledged, update_id)
    stop_at = time.perf_counter() + seconds
    reads: list[tuple[float, float, str, str | None, int]] = []            # start, latency, key, update_id, retries
    lock = threading.Lock()
    t_base = datetime.now(timezone.utc)

    def writer() -> None:
        n = 0
        while time.perf_counter() < stop_at:
            code, price = updates[n % len(updates)]
            n += 1
            uid = f"{run_id}-{n:05d}"
            put_update(ddb, table, code, store, price + n * 0.01, iso(t_base + timedelta(seconds=n)), uid, if_newer=True)
            acked[code].append((time.perf_counter(), uid))
            time.sleep(1 / writes_per_s)

    def user(i: int) -> None:
        r, mine = random.Random(f"{seed}:{i}"), []
        while time.perf_counter() < stop_at:
            key = r.choice(keys)
            t = time.perf_counter()
            resp = ddb.get_item(TableName=table, Key={"product_code": {"S": key}, "store": {"S": store}},
                                ConsistentRead=consistent)
            mine.append((t, time.perf_counter() - t, key, resp.get("Item", {}).get("update_id", {}).get("S"),
                         resp["ResponseMetadata"].get("RetryAttempts", 0)))
            time.sleep(r.uniform(0.05, 0.25))
        with lock:
            reads.extend(mine)

    w = threading.Thread(target=writer)
    w.start()
    with ThreadPoolExecutor(max_workers=users) as pool:
        list(pool.map(user, range(users)))
    w.join()

    staleness = []
    for start, _, key, got, _ in reads:
        before = [(t, uid) for t, uid in acked[key] if t < start]
        if not before or before[-1][1] == got:
            staleness.append(0.0)
            continue
        ids = [uid for _, uid in acked[key]]
        newer = [t for t, uid in before if got is None or (got in ids and ids.index(uid) > ids.index(got))]
        staleness.append(start - min(newer) if newer else 0.0)
    lat = sorted(x[1] * 1000 for x in reads)
    return {"users": users, "seconds": seconds, "reads": len(reads), "writes": sum(len(v) for v in acked.values()),
            "consistent_read": consistent, "p50_ms": round(lat[len(lat) // 2], 1),
            "p99_ms": round(lat[int(len(lat) * 0.99) - 1], 1), "max_ms": round(lat[-1], 1),
            "stale_answers": sum(s > 0 for s in staleness), "max_staleness_ms": round(max(staleness) * 1000, 1),
            "retried_reads": sum(x[4] > 0 for x in reads), "client": where_am_i()}


def load_lines(r: dict, region: str) -> list[str]:
    kind = "strongly consistent" if r["consistent_read"] else "eventually consistent"
    return [f"client: {r['client']}; table in {region}; {kind} reads",
            f"{r['users']} clients, {r['reads']} reads and {r['writes']} writes in {r['seconds']:.0f} s: "
            f"p50 {r['p50_ms']} ms, p99 {r['p99_ms']} ms, slowest {r['max_ms']} ms; {r['retried_reads']} reads retried",
            f"stale answers: {r['stale_answers']} of {r['reads']}; largest staleness {r['max_staleness_ms']} ms"]
