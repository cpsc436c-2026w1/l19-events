"""Local stand-ins for what the handlers read on AWS: S3 objects, the 7-day baseline, the job's start time, the
store's export log, the Glue Data Catalog and the handlers' own stored state. Each reads the generated files in
events/. A handler holds one of these in a module variable, where Lambda code holds its boto3 clients.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from urllib.parse import unquote_plus

ROOT = Path(__file__).parent
EVENTS = ROOT / "events"


def manifest() -> dict:
    return json.loads((EVENTS / "manifest.json").read_text())


class LocalLake:
    """The raw zone for one batch case, plus what surrounds it."""

    def __init__(self, case: str, events: Path = EVENTS):
        self.case, self.folder = case, events / "batch-files" / case
        self.base = json.loads((events / "batch-files" / "baseline.json").read_text())
        self.job = json.loads((events / "manifest.json").read_text())["job"]

    def s3_events(self) -> list[dict]:
        return json.loads((self.folder / "s3-events.json").read_text())

    def get_object(self, key: str) -> bytes:                        # S3 GetObject
        return (self.folder / key).read_bytes()

    def list_keys(self, prefix: str, at: str) -> list[str]:         # S3 ListObjectsV2, as the prefix is at time `at`
        arrived = {object_key(r): r["eventTime"][:19] + "Z" for e in self.s3_events() for r in e["Records"]}
        return sorted(k for k, t in arrived.items() if k.startswith(prefix) and t <= at)

    def baseline(self, store: str) -> dict:                         # the 7 days before: {"days": [...], "usual": {...}}
        return self.base["per_store"][store]

    def job_started_at(self) -> str:                                # Glue GetJobRuns StartedOn, or the schedule
        return self.job["started_at"]

    def export_log(self, store: str) -> list[str]:
        path = self.folder / "export-logs" / f"{store}.log"
        return path.read_text().splitlines() if path.exists() else []

    def usual_product_codes(self, store: str) -> list[str]:        # the product codes in the store's earlier files
        with open(ROOT / "seed-data" / "products.csv", newline="") as f:
            return sorted(r["code"] for r in csv.DictReader(f))


def object_key(record: dict) -> str:
    """The key of one S3 event record. It arrives URL encoded (day%3D2026-10-31)."""
    return unquote_plus(record["s3"]["object"]["key"])


class LocalCatalog:
    """The Glue Data Catalog for one catalog case, and the definitions the watcher stored earlier."""

    def __init__(self, case: str, events: Path = EVENTS):
        self.folder = events / "catalog-changes" / case
        self.job_reads = json.loads((events / "manifest.json").read_text())["job"]["reads"]
        after = json.loads((self.folder / "after.json").read_text())
        before_path = self.folder / "before.json"
        self.current = {after["Table"]["Name"]: after}
        self.stored: dict[str, dict] = {}
        if before_path.exists():
            before = json.loads(before_path.read_text())
            self.stored[before["Table"]["Name"]] = before

    def events(self) -> list[dict]:
        return json.loads((self.folder / "events.json").read_text())

    def get_table(self, database: str, table: str) -> dict | None:  # Glue GetTable (and GetPartitions)
        return self.current.get(table)

    def tables(self) -> list[str]:
        return sorted(set(self.current) | set(self.stored))

    def samples(self, table: str, column: str) -> list:
        path = self.folder / "samples.json"
        if not path.exists():
            return []
        s = json.loads(path.read_text())
        return s["values"] if (s["table"], s["column"]) == (table, column) else []
