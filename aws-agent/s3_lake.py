"""What the agent's function reads, on S3: the same interface as local.LocalLake, so batch_check.py and
half_file_agent.py run unchanged in Lambda.

Objects in the bucket (`agent-up` and `agent-run` put them there):
    prices_raw/day=<day>/<store>-export.csv    a store's export file; a new one starts the function
    export-logs/day=<day>/<store>.log          the store's export log for that file
    reference/baseline.json, history.json      the 7 days before: usual values, and rows per product
    reference/products.csv, job.json           the products table, and the nightly job (name, start time)
    results/day=<day>/<store>.json             what the function decided, for a person to read

The case is dated 2026-10-31, and the upload happens today. So the file carries the simulator's arrival time as
object metadata (`simulated-arrival`), and the function uses it in place of the upload time when it is there.
"""
from __future__ import annotations

import csv
import io
import json
from urllib.parse import quote


class S3Lake:
    def __init__(self, s3, bucket: str):
        self.s3, self.bucket, self._cache = s3, bucket, {}

    def _get(self, key: str) -> bytes:
        if key not in self._cache:
            self._cache[key] = self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return self._cache[key]

    def _json(self, key: str) -> dict:
        return json.loads(self._get(key))

    def get_object(self, key: str) -> bytes:
        return self._get(key)

    def arrived_at(self, key: str, event_time: str) -> str:
        """The simulator's arrival time when the object carries one, else the time of the S3 event."""
        meta = self.s3.head_object(Bucket=self.bucket, Key=key).get("Metadata", {})
        return meta.get("simulated-arrival", event_time)

    def s3_events(self) -> list[dict]:
        """Arrival times of the day's files, in the shape of S3 event records (half_file_agent's list_files tool
        reads them this way from the local event files)."""
        out = []
        for key in list(self._cache):
            if key.startswith("prices_raw/"):
                out.append({"Records": [{"eventTime": self.arrived_at(key, "")[:19] + ".000Z",
                                         "s3": {"object": {"key": quote(key, safe="/")}}}]})
        return out

    def baseline(self, store: str) -> dict:
        return self._json("reference/baseline.json")["per_store"][store]

    def job_started_at(self) -> str:
        return self._json("reference/job.json")["started_at"]

    def export_log(self, store: str) -> list[str]:
        day = self.job_started_at()[:10]
        try:
            return self._get(f"export-logs/day={day}/{store}.log").decode().splitlines()
        except Exception:                  # no log for this store (or no permission to see that it is missing)
            return []

    def rows_per_product(self, store: str, day: str) -> dict[str, int] | None:
        if day == self.job_started_at()[:10]:
            try:
                data = self._get(f"prices_raw/day={day}/{store}-export.csv")
            except Exception:
                return None
            counts: dict[str, int] = {}
            for r in csv.DictReader(io.StringIO(data.decode())):
                counts[r["product_code"]] = counts.get(r["product_code"], 0) + 1
            return counts
        return self._json("reference/history.json").get(store, {}).get(day)

    def product_categories(self) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        for r in csv.DictReader(io.StringIO(self._get("reference/products.csv").decode())):
            out.setdefault(r["category"], []).append(r["code"])
        return out
