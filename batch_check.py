"""New export file in prices_raw: the checks a team can write now, with a fixed verdict.

A Lambda function on the S3 event notification for prices_raw/. It runs before the nightly job reads the file.

    verdict       pass | hold | rerun_needed | unsettled
    cause         no_fault | duplicate_rows | currency_not_converted | late_arrival | None when unsettled
    unsettled     a check failed and no rule names the cause: half_file_agent.py takes it from there

`deadline_handler` runs on a schedule at 01:55: a store whose file did not arrive sends no event.

    uv run python batch_check.py                  # every batch case
    uv run python batch_check.py half-file
"""
from __future__ import annotations

import csv
import io
import statistics
import sys

from local import LocalLake, manifest, object_key
from results import FIXES, FileFacts, FileVerdict, MissingVerdict

STORE_CURRENCY = {"north": "CAD", "centre": "CAD", "online": "EUR"}
EUR_TO_CAD = 1.55
ROWS_TOLERANCE, PRICE_TOLERANCE = 0.05, 0.08       # L18 triage/world.py alarm_clear

LAKE: LocalLake | None = None                      # set by the caller; boto3 clients in AWS


def read_rows(data: bytes) -> list[dict]:
    return list(csv.DictReader(io.StringIO(data.decode())))


def facts_of(rows: list[dict], size: int, arrived_at: str) -> FileFacts:
    ids = [r["obs_id"] for r in rows]
    # each price converted by its own row's currency label, as the job converts it
    cad = [float(r["price"]) * (EUR_TO_CAD if r["currency"] == "EUR" else 1) for r in rows]
    return FileFacts(rows=len(rows), products=len({r["product_code"] for r in rows}),
                     repeated_obs_ids=len(ids) - len(set(ids)), avg_price_cad=round(statistics.fmean(cad), 2),
                     currencies=sorted({r["currency"] for r in rows}), size=size, arrived_at=arrived_at)


def check(key: str, data: bytes, arrived_at: str, usual: dict, job_started_at: str) -> FileVerdict:
    """The runbook for one file. The first rule that fires names the cause."""
    store, day = key.rsplit("/", 1)[1].split("-export")[0], key.split("day=")[1].split("/")[0]
    f = facts_of(read_rows(data), len(data), arrived_at)

    def verdict(v, cause, reasons, approval=False):
        return FileVerdict(key=key, store=store, day=day, verdict=v, cause=cause, fix=FIXES[cause] if cause else None,
                           needs_approval=approval, reasons=reasons, facts=f)

    rows_off = abs(f.rows / usual["rows"] - 1) > ROWS_TOLERANCE
    price_ratio = f.avg_price_cad / usual["avg_price_cad"]
    if f.repeated_obs_ids:
        return verdict("hold", "duplicate_rows", [f"{f.repeated_obs_ids} obs_id values appear more than once"])
    if f.currencies != [STORE_CURRENCY[store]]:
        if abs(price_ratio - 1 / EUR_TO_CAD) < 0.05 or abs(price_ratio - EUR_TO_CAD) < 0.05:
            return verdict("hold", "currency_not_converted",
                           [f"currency label {f.currencies}, expected {STORE_CURRENCY[store]}",
                            f"average in CAD {f.avg_price_cad} against the usual {usual['avg_price_cad']}"])
        return verdict("unsettled", None, [f"currency label {f.currencies}, expected {STORE_CURRENCY[store]}"])
    if rows_off or abs(price_ratio - 1) > PRICE_TOLERANCE:
        reasons = ([f"{f.rows} rows against the usual {usual['rows']:.0f}"] if rows_off else []) + (
            [f"average in CAD {f.avg_price_cad} against the usual {usual['avg_price_cad']}"]
            if abs(price_ratio - 1) > PRICE_TOLERANCE else [])
        return verdict("unsettled", None, reasons)
    if arrived_at > job_started_at:
        return verdict("rerun_needed", "late_arrival", [f"arrived {arrived_at}, the job started {job_started_at}"],
                       approval=True)
    return verdict("pass", "no_fault", [])


def handler(event: dict, context=None) -> list[dict]:
    """One FileVerdict per record of the S3 event."""
    out = []
    for record in event["Records"]:
        key = object_key(record)
        store = key.rsplit("/", 1)[1].split("-export")[0]
        arrived = record["eventTime"][:19] + "Z"
        out.append(check(key, LAKE.get_object(key), arrived, LAKE.baseline(store)["usual"],
                         LAKE.job_started_at()).model_dump())
    return out


def deadline_handler(event: dict, context=None) -> dict:
    """On a schedule before the job: which stores have no file for the day yet?
    event: {"day": "2026-10-31", "time": "2026-10-31T01:55:00Z"} (the time the schedule fired)."""
    have = {k.rsplit("/", 1)[1].split("-export")[0]
            for k in LAKE.list_keys(f"prices_raw/day={event['day']}/", event["time"])}
    missing = sorted(set(STORE_CURRENCY) - have)
    return MissingVerdict(day=event["day"], missing_stores=missing, cause="missing_input" if missing else None,
                          fix=FIXES["missing_input"] if missing else None).model_dump()


if __name__ == "__main__":
    wanted = sys.argv[1:] or [c["id"] for c in manifest()["cases"] if c["kind"] == "batch-files"]
    for case in wanted:
        LAKE = LocalLake(case)
        for e in LAKE.s3_events():
            for v in handler(e):
                print(f"{case:15} {v['store']:7} {v['verdict']:13} {v['cause'] or '-':24} {'; '.join(v['reasons'])}")
        missing = deadline_handler({"day": manifest()["day"], "time": manifest()["day"] + "T01:55:00Z"})
        if missing["missing_stores"]:
            print(f"{case:15} at 01:55: no file from {missing['missing_stores']} -> {missing['fix']}")
