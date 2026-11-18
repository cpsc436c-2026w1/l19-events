"""Generate the L19 event examples: three kinds of events on the grocery-price case of L18, with planted cases.

    uv run python make_events.py            # writes events/ (the same bytes on every run)
    uv run pytest                           # determinism and shape tests

One case, the stores and the lake of L18 (public/l18-planning-agent: triage/world.py, storeplan/). Three kinds:

    price-updates     change records of the latest-price DynamoDB table for one hour (DynamoDB Streams record shape)
    batch-files       one export file per store for one day of prices_raw, with its S3 event notification
    catalog-changes   EventBridge events of the Glue Data Catalog, with the table definition before and after

Nothing here calls AWS or a model. Every value is drawn from random.Random seeded with SEED and the name of the
part, so a run does not depend on the clock, on set order or on the order in which the parts are built.

Shapes, read in the AWS documentation on 2026-11-17 (a field the documentation does not show is marked `assumed`
in events/README.md):
    DynamoDB Streams record     Lambda developer guide, services-ddb-windows (DynamodbTimeWindowEvent example);
                                DynamoDB API reference, StreamRecord
    S3 event notification       S3 user guide, notification-content-structure (version 2.6; the object key is
                                URL encoded)
    Glue Data Catalog events    Glue developer guide, automating-awsglue-with-cloudwatch-events
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import random
import shutil
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).parent
OUT = ROOT / "events"
SEED = 19

# ---- the case: constants of L18 (public/l18-planning-agent) -------------------------------------------------
DAY = "2026-10-31"                                            # triage/world.py DAY
STORES = {"north": "CAD", "centre": "CAD", "online": "EUR"}   # storeplan/fixtures.py STORES: currency of the raw zone
PRODUCTS = 411                                                # triage/world.py PRODUCTS; data/products.jsonl
NORMAL_AVG = {"north": 6.12, "centre": 6.05, "online": 6.31}  # triage/world.py NORMAL_AVG, CAD, illustrative
EUR_TO_CAD = 1.55                                             # storeplan/fixtures.py, assumed, illustrative
USD_TO_CAD = 1.40                                             # storeplan/fixtures.py, assumed, illustrative
JOB = "curate-prices"
JOB_STARTED_AT = f"{DAY}T02:00:04Z"                           # triage/world.py _base: the nightly run
ARRIVALS = {"north": "01:40:10", "centre": "01:41:10", "online": "01:42:10"}   # triage/world.py _base
LATE_ARRIVAL = "03:12:40"                                     # triage/world.py late_arrival
DUPLICATES_ON_DAY = {"north": 82, "centre": 82, "online": 83}  # 247 in all: the job log of L18 reads 1480 rows
HISTORY = [f"2026-10-{d:02d}" for d in range(24, 31)]         # triage/world.py HISTORY: the 7 days before DAY
ROWS_TOLERANCE, PRICE_TOLERANCE = 0.05, 0.08                  # triage/world.py World.alarm_clear
RAW_COLUMNS = [("obs_id", "string", "id of the observation (large, unique)"),      # storeplan/catalog.json
               ("product_code", "string", "product code"),
               ("store", "string", "north, centre or online"),
               ("observed_on", "date", "day of the observation (DATE)"),
               ("price", "double", "price in the store's currency"),
               ("currency", "string", "CAD or EUR"),
               ("source", "string", "receipt reference (large, unique)")]
CURATED_COLUMNS = [("product_code", "string", "product code"), ("store", "string", "north, centre or online"),
                   ("observed_on", "date", "day of the price (DATE)"), ("price_cad", "double", "price in CAD")]
JOB_READS = ["product_code", "store", "observed_on", "price", "currency"]   # storeplan/lake.py: the curated SELECT
DATABASE = "l18_storeplan"                                    # storeplan/backends.py AWS_NAMES

# ---- placeholders (not from L18) ----------------------------------------------------------------------------
ACCOUNT = "123456789012"                                      # the account id of the AWS documentation examples
REGION = "us-west-2"
BUCKET = "lake"                                               # triage/world.py logs read s3://lake/prices_raw/
TABLE = "latest_price"                                        # assumed name of the latest-price table
STREAM_ARN = f"arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{TABLE}/stream/2026-10-01T00:00:00.000"

# ---- the hour of price updates ------------------------------------------------------------------------------
HOUR_START = datetime(2026, 10, 31, 9, 0, 0, tzinfo=timezone.utc)
WINDOW_S = 300                                                # tumbling window of 5 min (plans: l19-stream-agents)
UPDATES_PER_HOUR = 60                                         # per store, assumed
OFFLINE = ("centre", 10 * 60, 16 * 60)                        # centre does not send from 09:10 to 09:16, then catches up
BURST = ("north", 20 * 60, 120)                               # north: 120 updates, one a second from 09:20:00
QUIET = (40 * 60, 50 * 60)                                    # 09:40 to 09:50: no store sends an update
REDELIVERED = (27 * 60, 4)                                    # the first 4 records written from 09:27 arrive twice
EUR_AT = 33 * 60                                              # the first online update from 09:33 carries EUR


def rng(part: str) -> random.Random:
    return random.Random(f"{SEED}:{part}")


def md5(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def dump(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=1, sort_keys=True, ensure_ascii=True) + "\n")


def rel(path: Path) -> str:
    return "events/" + path.relative_to(OUT).as_posix()


# ---- prices -------------------------------------------------------------------------------------------------

def base_prices() -> dict[str, dict[str, float]]:
    """CAD price per store and product: the products' base price times a store factor, scaled so that each
    store's average is L18's NORMAL_AVG."""
    with open(ROOT / "seed-data" / "products.csv", newline="") as f:
        products = [(r["code"], float(r["price_usd"])) for r in csv.DictReader(f)]
    assert len(products) == PRODUCTS
    r = rng("prices")
    out = {}
    for store in STORES:
        raw = {code: usd * USD_TO_CAD * (0.94 + r.random() * 0.12) for code, usd in products}
        scale = NORMAL_AVG[store] / statistics.fmean(raw.values())
        out[store] = {code: round(p * scale, 2) for code, p in raw.items()}
    return out


# ---- kind b: batch files ------------------------------------------------------------------------------------

def export_rows(store: str, day: str, base: dict[str, float], duplicates: int | None = None) -> list[dict]:
    """One store's export for one day: a row per product, some observed twice (storeplan/lake.py: a second
    observation has its own obs_id and receipt), in the store's own currency, with daily noise of -3 to +3 %."""
    r = rng(f"file:{store}:{day}")
    codes = sorted(base)
    twice = set(r.sample(codes, duplicates if duplicates is not None else r.randint(70, 95)))
    currency = STORES[store]
    rows = []
    for code in codes:
        cad = base[code] * (1 + r.uniform(-0.03, 0.03))
        price = round(cad / EUR_TO_CAD if currency == "EUR" else cad, 2)
        for n in range(2 if code in twice else 1):
            rows.append({"obs_id": md5(f"{code}{store}{day}{n}"), "product_code": code, "store": store,
                         "observed_on": day, "price": f"{price:.2f}", "currency": currency,
                         "source": "receipt:" + md5(f"r{code}{store}{day}{n}")})
    return rows


def csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=[c[0] for c in RAW_COLUMNS], lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode()


def file_facts(rows: list[dict], data: bytes, arrived_at: str) -> dict:
    """What a check in code can compute from one file without a model."""
    ids = [r["obs_id"] for r in rows]
    return {"rows": len(rows), "products": len({r["product_code"] for r in rows}),
            "repeated_obs_ids": len(ids) - len(set(ids)),
            "avg_price": round(statistics.fmean(float(r["price"]) for r in rows), 2),
            # converted by each row's own currency label, as the job converts: comparable across stores
            "avg_price_cad": round(statistics.fmean(
                float(r["price"]) * (EUR_TO_CAD if r["currency"] == "EUR" else 1) for r in rows), 2),
            "currencies": sorted({r["currency"] for r in rows}), "size": len(data), "arrived_at": arrived_at,
            "arrived_after_job_start": arrived_at > JOB_STARTED_AT}


def s3_event(key: str, data: bytes, arrived_at: str) -> dict:
    """One S3 event notification message for a PUT (notification-content-structure, version 2.6)."""
    t = datetime.strptime(arrived_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    h = md5(key + arrived_at)
    return {"Records": [{
        "eventVersion": "2.6", "eventSource": "aws:s3", "awsRegion": REGION,
        "eventTime": arrived_at[:-1] + ".000Z", "eventName": "ObjectCreated:Put",
        "userIdentity": {"principalId": "AWS:AROAEXAMPLEEXPORT:store-export"},
        "requestParameters": {"sourceIPAddress": "172.16.0.1"},
        "responseElements": {"x-amz-request-id": h[:16].upper(), "x-amz-id-2": h + h[::-1]},
        "s3": {"s3SchemaVersion": "1.0", "configurationId": "new-raw-file",
               "bucket": {"name": BUCKET, "ownerIdentity": {"principalId": "A3NL1KOZZKExample"},
                          "arn": f"arn:aws:s3:::{BUCKET}"},
               "object": {"key": quote(key, safe="/"),          # URL encoded: day=... reads day%3D...
                          "size": len(data), "eTag": hashlib.md5(data).hexdigest(),
                          "sequencer": f"{int(t.timestamp() * 1000):016X}{h[:4].upper()}"}}}]}


def export_log(store: str, arrived: str, query_rows: int, query_products: int, size: int) -> list[str]:
    """The store's export log for one night (a design for this demo). It says what the export read and what it
    uploaded; it does not name a fault."""
    t = datetime.strptime(arrived, "%Y-%m-%dT%H:%M:%SZ")
    at = lambda s: (t - timedelta(seconds=s)).strftime("%H:%M:%S")   # noqa: E731
    return [f"{at(12)} INFO export started for store {store}, day={DAY}",
            f"{at(7)} INFO query returned {query_rows} rows for {query_products} products",
            f"{at(1)} INFO upload finished: {size} bytes"]


FILTERED_CATEGORY = "noodles"                                 # 24 of the 411 products (L18 data/products.jsonl)


def categories() -> dict[str, str]:
    with open(ROOT / "seed-data" / "products.csv", newline="") as f:
        return {r["code"]: r["category"] for r in csv.DictReader(f)}


BATCH_CASES = {
    "normal-day": "Three exports arrive before the job starts; rows and prices are in their usual range.",
    "duplicate-rows": "Export of store centre holds every row twice: the same obs_id appears two times.",
    "missing-store": "Export of store online does not arrive: two files, two events.",
    "not-converted": "Export of store online is labelled CAD, but its prices are the EUR amounts.",
    "half-file": "Export of store centre stops after half of the products; the file is valid CSV.",
    "category-missing": f"Export of store north lacks every product of category {FILTERED_CATEGORY}; the export's "
                        "query left them out.",
    "late-file": f"Export of store centre arrives at {LATE_ARRIVAL}, after the job started at 02:00:04.",
}


def build_batch_files(base: dict) -> tuple[list[dict], dict]:
    history, per_product = {}, {s: {} for s in STORES}
    category = categories()
    for day in HISTORY:
        for store in STORES:
            rows = export_rows(store, day, base[store])
            counts: dict[str, int] = {}
            for r in rows:
                counts[r["product_code"]] = counts.get(r["product_code"], 0) + 1
            per_product[store][day] = counts
            history.setdefault(store, []).append(
                {"day": day, **file_facts(rows, csv_bytes(rows), f"{day}T{ARRIVALS[store]}Z")})
    baseline = {"days": HISTORY, "job": JOB, "job_starts_at": "02:00:04Z",
                "tolerance": {"rows": ROWS_TOLERANCE, "avg_price": PRICE_TOLERANCE},
                "per_store": {s: {"days": d, "usual": {
                    k: statistics.median(x[k] for x in d) for k in ("rows", "products", "size", "avg_price", "avg_price_cad")}}
                    for s, d in history.items()}}
    dump(OUT / "batch-files" / "baseline.json", baseline)
    dump(OUT / "batch-files" / "history.json", per_product)          # rows per product in each earlier file

    cases = []
    for case, what in BATCH_CASES.items():
        folder = OUT / "batch-files" / case
        events, facts, files = [], {}, []
        for store in STORES:
            rows = export_rows(store, DAY, base[store], DUPLICATES_ON_DAY[store])
            arrived = f"{DAY}T{ARRIVALS[store]}Z"
            if case == "missing-store" and store == "online":
                continue
            if case == "duplicate-rows" and store == "centre":
                rows = rows + rows
            if case == "not-converted" and store == "online":
                rows = [{**r, "currency": "CAD"} for r in rows]
            query = (len(rows), len({r["product_code"] for r in rows}))      # what the export's query returned
            if case == "half-file" and store == "centre":
                keep = set(sorted(base[store])[:PRODUCTS // 2])              # the upload stops; the query was complete
                rows = [r for r in rows if r["product_code"] in keep]
            if case == "category-missing" and store == "north":
                rows = [r for r in rows if category[r["product_code"]] != FILTERED_CATEGORY]
                query = (len(rows), len({r["product_code"] for r in rows}))  # the query itself left them out
            if case == "late-file" and store == "centre":
                arrived = f"{DAY}T{LATE_ARRIVAL}Z"
            key = f"prices_raw/day={DAY}/{store}-export.csv"
            data = csv_bytes(rows)
            path = folder / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            files.append(rel(path))
            facts[store] = file_facts(rows, data, arrived)
            log = folder / "export-logs" / f"{store}.log"
            log.parent.mkdir(parents=True, exist_ok=True)
            log.write_text("\n".join(export_log(store, arrived, *query, len(data))) + "\n")
            files.append(rel(log))
            events.append((arrived, s3_event(key, data, arrived)))
        events = [e for _, e in sorted(events, key=lambda x: x[0])]         # in the order the files arrived
        dump(folder / "s3-events.json", events)
        cases.append({"id": case, "kind": "batch-files", "planted": case != "normal-day", "what": what,
                      "files": [rel(folder / "s3-events.json"), *files],
                      "facts": {"job_started_at": JOB_STARTED_AT, "events": len(events), "per_store": facts}})
    return cases, baseline


# ---- kind a: price updates ----------------------------------------------------------------------------------

def _image(code, store, price, currency, observed_at, update_id) -> dict:
    return {"product_code": {"S": code}, "store": {"S": store}, "price": {"N": f"{price:.2f}"},
            "currency": {"S": currency}, "observed_at": {"S": observed_at}, "update_id": {"S": update_id}}


def build_price_updates(base: dict) -> list[dict]:
    """One hour of change records of the latest-price table. The table holds one item per product and store,
    always in CAD: the writer converts store online's EUR before it writes (so EUR in the table is a fault).
    `observed_at` is when the store changed the price; ApproximateCreationDateTime is when the write reached the
    table. Lambda's tumbling windows use the second (services-ddb-windows)."""
    r = rng("updates")
    held_out = {("online", c) for c in sorted(base["online"])[-3:]}      # not in the table yet: their first write is an INSERT
    updates = []                                                         # (written_s, observed_s, store, code, kind)
    for store in STORES:
        t = 0.0
        pool = [c for c in sorted(base[store]) if (store, c) not in held_out]
        while True:
            t += r.expovariate(UPDATES_PER_HOUR / 3600)
            if t >= 3600:
                break
            obs = int(t)
            if QUIET[0] <= obs < QUIET[1]:
                continue
            written = min(obs + r.randint(1, 3), (obs // WINDOW_S + 1) * WINDOW_S - 1)   # a normal write stays in its window
            kind = "normal"
            if store == OFFLINE[0] and OFFLINE[1] <= obs < OFFLINE[2]:
                written = OFFLINE[2] + 5 + len([u for u in updates if u[4] in ("late", "held")])
                kind = "late" if obs // WINDOW_S < written // WINDOW_S else "held"
            updates.append((written, obs, store, r.choice(pool), kind))
    for i, code in enumerate(sorted(c for s, c in held_out)):            # three new products at store online
        obs = 5 * 60 + i * 15 * 60 + 7
        updates.append((obs + 2, obs, "online", code, "normal"))
    burst_codes = r.sample(sorted(base[BURST[0]]), BURST[2])
    for i, code in enumerate(burst_codes):
        updates.append((BURST[1] + i + 1, BURST[1] + i, BURST[0], code, "burst"))
    updates.sort(key=lambda u: (u[0], u[1], u[2], u[3]))

    eur_done = False
    table = {(s, c): _image(c, s, p, "CAD", f"{DAY}T08:00:00Z", f"load-{DAY}")
             for s in STORES for c, p in base[s].items() if (s, c) not in held_out}
    records, kinds = [], []
    for n, (written, obs, store, code, kind) in enumerate(updates, start=1):
        old = table.get((store, code))
        cad = float(old["price"]["N"]) if old else base[store][code]
        new_price = round(cad * (0.90 if kind == "burst" else 1 + r.uniform(-0.08, 0.08)), 2)
        currency = "CAD"
        if store == "online" and kind == "normal" and obs >= EUR_AT and not eur_done and old:
            new_price, currency, kind, eur_done = round(new_price / EUR_TO_CAD, 2), "EUR", "eur", True
        new = _image(code, store, new_price, currency, iso(HOUR_START + timedelta(seconds=obs)), f"upd-{n:05d}")
        table[(store, code)] = new
        seq = f"{4_000_000_000_000_000_000_000 + n * 100:d}"
        body = {"ApproximateCreationDateTime": int((HOUR_START + timedelta(seconds=written)).timestamp()),
                "Keys": {"product_code": {"S": code}, "store": {"S": store}}, "NewImage": new,
                "SequenceNumber": seq, "StreamViewType": "NEW_AND_OLD_IMAGES"}
        if old:
            body["OldImage"] = old
        body["SizeBytes"] = len(json.dumps({k: body[k] for k in ("Keys", "NewImage", "OldImage") if k in body},
                                           separators=(",", ":")))
        records.append({"eventID": md5(seq), "eventName": "MODIFY" if old else "INSERT", "eventVersion": "1.1",
                        "eventSource": "aws:dynamodb", "awsRegion": REGION, "dynamodb": body,
                        "eventSourceARN": STREAM_ARN})
        kinds.append(kind)

    # delivery order: the stream holds each record once; Lambda hands a batch to the function again after a
    # failed invocation, so a function can receive a record twice (services-ddb-windows: at least once).
    first = next(i for i, u in enumerate(updates) if u[0] >= REDELIVERED[0])
    again = list(range(first, first + REDELIVERED[1]))
    delivered = list(range(first + REDELIVERED[1])) + again + list(range(first + REDELIVERED[1], len(records)))
    folder = OUT / "price-updates"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "records.jsonl").write_text(
        "".join(json.dumps(records[i], sort_keys=True) + "\n" for i in delivered))

    def window(seconds: int) -> int:
        return seconds // WINDOW_S

    def summary(rows):                                   # rows: (store, price)
        out = {}
        for store in STORES:
            p = [x for s, x in rows if s == store]
            out[store] = {"count": len(p), "avg_price": round(statistics.fmean(p), 2) if p else None}
        return out

    windows = []
    for w in range(3600 // WINDOW_S):
        truth = [(u[2], float(rec["dynamodb"]["NewImage"]["price"]["N"]))
                 for u, rec, k in zip(updates, records, kinds) if window(u[1]) == w and k != "eur"]
        naive = [(updates[i][2], float(records[i]["dynamodb"]["NewImage"]["price"]["N"]))
                 for i in delivered if window(updates[i][0]) == w]
        windows.append({"start": iso(HOUR_START + timedelta(seconds=w * WINDOW_S)),
                        "end": iso(HOUR_START + timedelta(seconds=(w + 1) * WINDOW_S)),
                        "correct": summary(truth), "as_delivered": summary(naive)})
    ids = lambda k: [rec["eventID"] for rec, kk in zip(records, kinds) if kk == k]   # noqa: E731
    planted = {"redelivered-update": [records[i]["eventID"] for i in again], "late-update": ids("late"),
               "eur-update": ids("eur"), "store-burst": ids("burst")}
    dump(folder / "truth.json", {
        "window_seconds": WINDOW_S,
        "correct": "each update once, in the window of its observed_at, CAD only (the EUR update is left out)",
        "as_delivered": "every delivered record, in the window of its ApproximateCreationDateTime, any currency",
        "records_in_stream": len(records), "records_delivered": len(delivered), "windows": windows,
        "planted_event_ids": planted,
        "quiet": {"from": iso(HOUR_START + timedelta(seconds=QUIET[0])),
                  "to": iso(HOUR_START + timedelta(seconds=QUIET[1]))}})
    # one invocation as Lambda sends it with a tumbling window: the first three records of the 09:15 window
    w0 = 15 * 60
    in_w = [records[i] for i, u in enumerate(updates) if w0 <= u[0] < w0 + WINDOW_S][:3]
    dump(folder / "window-event.json", {
        "Records": in_w, "window": {"start": iso(HOUR_START + timedelta(seconds=w0)),
                                    "end": iso(HOUR_START + timedelta(seconds=w0 + WINDOW_S))},
        "state": {}, "shardId": "shardId-00000001730365200000-a1b2c3d4", "eventSourceARN": STREAM_ARN,
        "isFinalInvokeForWindow": False, "isWindowTerminatedEarly": False})

    # three records that do not fit the input model, and one batch that holds one of them
    def broken(i: int, n: int, change) -> dict:
        r = json.loads(json.dumps(records[i]))
        seq = f"{4_100_000_000_000_000_000_000 + n * 100:d}"
        r["eventID"], r["dynamodb"]["SequenceNumber"] = md5(seq), seq
        r["dynamodb"]["NewImage"]["update_id"] = {"S": f"bad-{n:05d}"}
        change(r["dynamodb"]["NewImage"])
        return r
    w5 = [i for i, u in enumerate(updates) if 5 * 60 <= u[0] < 10 * 60][:7]      # seven good records of the 09:05 window
    invalid = [broken(w5[0], 1, lambda img: img.pop("price")),
               broken(w5[1], 2, lambda img: img.update(price={"S": "four dollars"})),
               broken(w5[2], 3, lambda img: img.update(store={"S": "south"}))]
    (folder / "invalid-records.jsonl").write_text("".join(json.dumps(r, sort_keys=True) + "\n" for r in invalid))
    batch = [records[i] for i in w5]
    batch.insert(5, invalid[0])                                                    # the sixth of eight records
    dump(folder / "failing-batch.json", {
        "Records": batch, "window": {"start": iso(HOUR_START + timedelta(seconds=300)),
                                     "end": iso(HOUR_START + timedelta(seconds=600))},
        "state": {}, "shardId": "shardId-00000001730365200000-a1b2c3d4", "eventSourceARN": STREAM_ARN,
        "isFinalInvokeForWindow": False, "isWindowTerminatedEarly": False})

    files = [rel(folder / "records.jsonl"), rel(folder / "truth.json")]
    n_normal = kinds.count("normal") + kinds.count("held")
    burst_end = iso(HOUR_START + timedelta(seconds=BURST[1] + BURST[2]))
    what = {
        "normal-update": (False, f"{n_normal} updates written within 3 s of the price change "
                          f"({sum(1 for x in records if x['eventName'] == 'INSERT')} of them a product new to store online)."),
        "redelivered-update": (True, f"{len(again)} records delivered a second time: same eventID and SequenceNumber."),
        "late-update": (True, f"{len(planted['late-update'])} updates of store centre observed from 09:10 to 09:15 and "
                        "written after 09:16, in the next window."),
        "eur-update": (True, "One update of store online with currency EUR and the EUR amount; the table holds CAD."),
        "store-burst": (True, f"{BURST[2]} updates of store north from 09:20:00 to {burst_end[11:19]}, each price 10 % lower."),
        "quiet-windows": (True, "No update from any store from 09:40 to 09:50: two windows without records."),
    }
    out = [{"id": cid, "kind": "price-updates", "planted": p, "what": w, "files": files,
            "facts": {"event_ids": planted.get(cid, []), "records": len(planted.get(cid, [])) or
                      (n_normal if cid == "normal-update" else 0)}} for cid, (p, w) in what.items()]
    out.append({"id": "invalid-update", "kind": "price-updates", "planted": True,
                "what": "Three records that do not fit the input model: a missing price, a price as text, an unknown store.",
                "files": [rel(folder / "invalid-records.jsonl")],
                "facts": {"event_ids": [r["eventID"] for r in invalid], "records": 3,
                          "fields": ["price", "price", "store"]}})
    out.append({"id": "failing-batch", "kind": "price-updates", "planted": True,
                "what": "One batch of 8 records for the 09:05 window; the sixth lacks a price, so a function that "
                        "does not check its input raises on it.",
                "files": [rel(folder / "failing-batch.json")],
                "facts": {"event_ids": [invalid[0]["eventID"]], "records": 8, "bad_position": 6}})
    return out


# ---- kind c: catalog changes --------------------------------------------------------------------------------

def table_def(name: str, columns, version: int, updated: str, partition_keys=(), partitions=None) -> dict:
    """A table as Glue GetTable returns it, cut to the fields a handler compares."""
    out = {"Table": {"Name": name, "DatabaseName": DATABASE, "VersionId": str(version), "UpdateTime": updated,
                     "TableType": "EXTERNAL_TABLE",
                     "StorageDescriptor": {"Location": f"s3://{BUCKET}/{name}/",
                                           "Columns": [{"Name": n, "Type": t, "Comment": c} for n, t, c in columns]},
                     "PartitionKeys": [{"Name": n, "Type": t, "Comment": c} for n, t, c in partition_keys]}}
    if partitions is not None:
        out["PartitionValues"] = [[p] for p in partitions]
    return out


def glue_event(detail_type: str, time: str, detail: dict, resource: str) -> dict:
    h = md5(detail_type + time + json.dumps(detail, sort_keys=True))
    return {"version": "0", "id": f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}", "detail-type": detail_type,
            "source": "aws.glue", "account": ACCOUNT, "time": time, "region": REGION,
            "resources": [f"arn:aws:glue:{REGION}:{ACCOUNT}:{resource}"], "detail": detail}


def table_event(table: str, change: str, time: str, partitions=None) -> dict:
    detail = {"databaseName": DATABASE, "tableName": table, "typeOfChange": change}
    if partitions is not None:
        detail["changedPartitions"] = partitions
    return glue_event("Glue Data Catalog Table State Change", time, detail, f"table/{DATABASE}/{table}")


def build_catalog_changes() -> list[dict]:
    v6 = "2026-09-02T10:11:00Z"                                         # triage/world.py: version 6 of prices_raw
    months = [f"2026-{m:02d}" for m in range(1, 11)]
    month_key = [("month", "string", "partition column, YYYY-MM")]
    raw = lambda cols, version, updated: table_def("prices_raw", cols, version, updated)   # noqa: E731

    def swap(cols, name, new):                                           # replace or drop one column
        return [x for c in cols for x in ([c] if c[0] != name else ([new] if new else []))]

    promo = ("on_promotion", "boolean", "")
    with_promo = RAW_COLUMNS + [promo]
    price_string = ("price", "string", "price in the store's currency")
    cases = {
        "new-partition": (False, "Nightly job writes the first day of November: prices_curated gets partition month=2026-11.",
                          table_def("prices_curated", CURATED_COLUMNS, 3, v6, month_key, months),
                          table_def("prices_curated", CURATED_COLUMNS, 3, v6, month_key, months + ["2026-11"]),
                          [table_event("prices_curated", "CreatePartition", "2026-11-01T02:06:51Z", ["2026-11"])]),
        "new-table": (False, "Table price_window_counts is created in the database.",
                      None,
                      table_def("price_window_counts", [("store", "string", ""), ("window_start", "timestamp", ""),
                                                        ("updates", "bigint", ""), ("avg_price_cad", "double", "")],
                                0, "2026-11-02T09:30:00Z"),
                      [glue_event("Glue Data Catalog Database State Change", "2026-11-02T09:30:00Z",
                                  {"databaseName": DATABASE, "typeOfChange": "CreateTable",
                                   "changedTables": ["price_window_counts"]}, f"database/{DATABASE}")]),
        "new-column": (True, "prices_raw gets column on_promotion (boolean), without a description.",
                       raw(RAW_COLUMNS, 6, v6), raw(with_promo, 7, "2026-11-02T10:00:00Z"),
                       [table_event("prices_raw", "UpdateTable", "2026-11-02T10:00:00Z")]),
        "type-change": (True, "Column price of prices_raw changes from double to string.",
                        raw(RAW_COLUMNS, 6, v6), raw(swap(RAW_COLUMNS, "price", price_string), 7, "2026-11-03T11:20:00Z"),
                        [table_event("prices_raw", "UpdateTable", "2026-11-03T11:20:00Z")]),
        "renamed-column": (True, "Column price of prices_raw becomes unit_price (the schema change of L18's triage).",
                           raw(RAW_COLUMNS, 6, v6),
                           raw(swap(RAW_COLUMNS, "price", ("unit_price", "double", "price in the store's currency")),
                               7, "2026-10-30T16:40:00Z"),
                           [table_event("prices_raw", "UpdateTable", "2026-10-30T16:40:00Z")]),
        "dropped-column": (True, "Column source (the receipt reference) is removed from prices_raw.",
                           raw(RAW_COLUMNS, 6, v6), raw(swap(RAW_COLUMNS, "source", None), 7, "2026-11-04T15:05:00Z"),
                           [table_event("prices_raw", "UpdateTable", "2026-11-04T15:05:00Z")]),
        "repeated-event": (True, "Two changes 40 s apart (a new column, then a type change): the second event arrives "
                           "first, then the first, then the second again.",
                           raw(RAW_COLUMNS, 6, v6),
                           raw(swap(with_promo, "price", price_string), 8, "2026-11-05T10:00:40Z"),
                           [table_event("prices_raw", "UpdateTable", t) for t in
                            ("2026-11-05T10:00:40Z", "2026-11-05T10:00:00Z", "2026-11-05T10:00:40Z")]),
        "no-event": (True, "Column currency of prices_raw is removed and no event arrives.",
                     raw(RAW_COLUMNS, 6, v6), raw(swap(RAW_COLUMNS, "currency", None), 7, "2026-11-06T13:00:00Z"), []),
    }
    out = []
    for cid, (planted, what, before, after, events) in cases.items():
        folder = OUT / "catalog-changes" / cid
        files = []
        for name, obj in (("events.json", events), ("before.json", before), ("after.json", after)):
            if obj is not None:
                dump(folder / name, obj)
                files.append(rel(folder / name))
        if cid == "new-column":
            r = rng("samples")
            dump(folder / "samples.json", {"table": "prices_raw", "column": promo[0], "type": promo[1],
                                           "values": [r.random() < 0.2 for _ in range(20)]})
            files.append(rel(folder / "samples.json"))
        if cid == "repeated-event":
            dump(folder / "between.json", raw(with_promo, 7, "2026-11-05T10:00:00Z"))
            files.append(rel(folder / "between.json"))
        cols = lambda t: {c["Name"]: c["Type"] for c in t["Table"]["StorageDescriptor"]["Columns"]} if t else {}   # noqa: E731
        b, a = cols(before), cols(after)
        out.append({"id": cid, "kind": "catalog-changes", "planted": planted, "what": what, "files": files,
                    "facts": {"events": len(events), "added": sorted(set(a) - set(b)) if before else [],
                              "removed": sorted(set(b) - set(a)),
                              "retyped": sorted(n for n in set(a) & set(b) if a[n] != b[n])}})
    return out


# ---- the README table ---------------------------------------------------------------------------------------

README_HEAD = """# Event examples for L19: grocery prices

Generated by `make_events.py` (seed {seed}); do not edit by hand. `uv run python make_events.py` writes the same
bytes again, and `uv run pytest` checks that. The case is the one of L18: stores north, centre and online, {products}
products, the lake tables `prices_raw` and `prices_curated`, the nightly Glue job `{job}` that starts at 02:00:04.
Prices are illustrative, as in L18; product codes and base prices in `../seed-data/products.csv` are copied from
L18's `data/products.jsonl` (code and price_usd). The columns the job reads are in `manifest.json` under `job.reads`
(from the curated SELECT in L18's `storeplan/lake.py`). What should happen for each case, and whether code or an agent does it:
`../TRIAGE.md`. Job stories and requirements: `../STORIES.md`. The same list for a program: `manifest.json`.

| Kind | Folder | One event is | Cases |
|---|---|---|---|
| Price updates | `price-updates/` | one change record of the latest-price DynamoDB table (item before and after) | {n_a} |
| Batch files | `batch-files/` | one S3 event notification for a new export file in `prices_raw/day={day}/` | {n_b} |
| Catalog changes | `catalog-changes/` | one EventBridge event of the Glue Data Catalog | {n_c} |

## Cases

| Case | Kind | Normal or planted | What is in it | Where |
|---|---|---|---|---|
{rows}

## Files

- `price-updates/records.jsonl`: {delivered} records in the order a Lambda function receives them, for the hour from
  09:00 to 10:00 UTC on {day}; the stream holds {in_stream} of them. `truth.json` names the planted records by
  `eventID` and gives, for each window of 5 minutes and each store, the correct count and average and the ones a
  function gets if it trusts the delivery. `window-event.json` is one invocation as Lambda sends it with a tumbling
  window.
- `batch-files/<case>/prices_raw/day={day}/<store>-export.csv`: the files of one night; `s3-events.json` holds their
  notifications in the order of arrival; `export-logs/<store>.log` is the store's export log for that file (a design
  for this demo: what the export read and what it uploaded). `baseline.json` has rows, size and average price of each store's file for
  the 7 days before, and `history.json` the rows per product in each of those files.
- `catalog-changes/<case>/events.json`: the events in the order of delivery; `before.json` and `after.json` are the
  table as `GetTable` returns it, cut to name, version, columns and partition keys. `new-column/samples.json` has 20
  values of the new column.

## What the AWS documentation says about each shape (read 2026-11-17)

- **DynamoDB Streams.** Each stream record appears exactly once in the stream; a write that does not change the item
  does not produce a record (DynamoDB developer guide, Streams). Lambda processes each record at least once, so a
  function can receive a record twice, and it sets the window boundaries from the time a record was inserted into
  the stream, not from a time inside the item (Lambda developer guide, services-ddb-windows). In these files
  `ApproximateCreationDateTime` is the insertion time and the attribute `observed_at` is when the store changed the
  price.
- **S3 event notifications.** The object key is URL encoded. Notifications are delivered at least once and can
  arrive out of order; `sequencer` is a hexadecimal value that orders events (S3 user guide,
  notification-content-structure and event ordering). `size` and `eTag` are the byte count and MD5 of the generated
  file.
- **Glue Data Catalog events.** `Glue Data Catalog Table State Change` carries `typeOfChange`, `databaseName`,
  `tableName` and `changedPartitions`; `Glue Data Catalog Database State Change` carries `typeOfChange`,
  `databaseName` and `changedTables`. The events do not carry columns: all the column cases look the same in
  `events.json` and differ only in `before.json` and `after.json`. The events are best effort; they can be out of
  order or missing (Glue developer guide, automating-awsglue-with-cloudwatch-events).

## Assumed, not shown in the documentation

- The Glue documentation shows one example event (`BatchCreatePartition`). That `UpdateTable` events leave out
  `changedPartitions`, and the form of `resources` for a table (`table/<database>/<table>`) and for a database
  (`database/<database>`), are assumed.
- The S3 documentation says the key is URL encoded and shows a space becoming `+`. A real S3 event on 2026-11-17
  carried `day%3D{day}` (`../output/aws-live/agent-run.txt`). The length and content of `sequencer` are assumed.
- Whether Lambda invokes a function for a window without records was not checked; the quiet windows only show that
  no record carries those 10 minutes.
- Account `{account}`, Region `{region}`, bucket `{bucket}`, the table name `{table}` and the stream ARN are
  placeholders. The latest-price table's attributes (`price`, `currency`, `observed_at`, `update_id`) are a design
  for this demo.
- {per_hour} updates an hour per store, the offline minutes of store centre, the burst of store north and the row
  counts of the 7 earlier days are chosen for the demo.
"""


def write_readme(cases: list[dict], truth: dict) -> None:
    where = {"price-updates": "`price-updates/records.jsonl`; ids in `truth.json` under `planted_event_ids`",
             "invalid-update": "`price-updates/invalid-records.jsonl`", "failing-batch": "`price-updates/failing-batch.json`",
             "batch-files": "`batch-files/{id}/`", "catalog-changes": "`catalog-changes/{id}/`"}
    rows = "\n".join(f"| `{c['id']}` | {c['kind']} | {'planted' if c['planted'] else 'normal'} | {c['what']} | "
                     f"{where.get(c['id'], where[c['kind']]).format(id=c['id'])} |" for c in cases)
    n = lambda k: sum(c["kind"] == k for c in cases)   # noqa: E731
    (OUT / "README.md").write_text(README_HEAD.format(
        seed=SEED, products=PRODUCTS, job=JOB, day=DAY, rows=rows, n_a=n("price-updates"), n_b=n("batch-files"),
        n_c=n("catalog-changes"), delivered=truth["records_delivered"], in_stream=truth["records_in_stream"],
        account=ACCOUNT, region=REGION, bucket=BUCKET, table=TABLE, per_hour=UPDATES_PER_HOUR))


def main(out: Path = OUT) -> list[dict]:
    global OUT
    OUT = out
    if OUT.exists():
        shutil.rmtree(OUT)
    base = base_prices()
    cases = build_price_updates(base)
    batch, _ = build_batch_files(base)
    cases += batch + build_catalog_changes()
    dump(OUT / "manifest.json", {"seed": SEED, "day": DAY, "cases": cases,
                                 "job": {"name": JOB, "started_at": JOB_STARTED_AT, "reads": JOB_READS,
                                         "eur_to_cad": EUR_TO_CAD}})
    write_readme(cases, json.loads((OUT / "price-updates" / "truth.json").read_text()))
    return cases


if __name__ == "__main__":
    for c in main():
        print(f"{c['kind']:16} {c['id']:20} {'planted' if c['planted'] else 'normal'}")
