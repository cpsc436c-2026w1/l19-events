"""Replay: rebuild the window numbers from the start of the stream and compare them with the stored ones.

The stored numbers came from the live delivery, in which 4 records arrived twice (Lambda delivers at least once).
A replay reads the stream from its start, where each record is once. A function that counts a record once however
often it arrives gives the same numbers both times. A function that does not skip a record it has seen
(dedupe=False) has 4 updates too many in its stored numbers, and the replay shows where.

    uv run python replay.py                   # the function as built: equal
    uv run python replay.py --no-dedupe       # the planted variant: the windows that differ
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from pydantic import BaseModel

import stream_window

RECORDS = Path(__file__).parent / "events" / "price-updates" / "records.jsonl"
FIRST, WINDOWS = "2026-10-31T09:00:00Z", 12


class ReplayReport(BaseModel):
    windows: int
    equal: bool
    differences: list[dict]                # window, store, stored and replayed count and average

    def line(self) -> str:
        if self.equal:
            return f"replay of {self.windows} windows from the start of the stream: equal to the stored numbers"
        d = "; ".join(f"{x['window'][11:16]} {x['store']} stored {x['stored']['count']} replayed {x['replayed']['count']}"
                      for x in self.differences)
        return f"replay of {self.windows} windows: {len(self.differences)} numbers differ: {d}"


def numbers(records: list[dict], **options) -> dict:
    stream_window.SINK = stream_window.Sink()
    stream_window.run_local(records, **options)
    return stream_window.final_numbers(stream_window.SINK, FIRST, WINDOWS)


def run(dedupe: bool = True) -> ReplayReport:
    delivered = [json.loads(line) for line in RECORDS.read_text().splitlines()]
    stored = numbers(delivered, dedupe=dedupe)                         # what the live delivery left in the store
    stream = list({r["eventID"]: r for r in delivered}.values())       # the stream: each record once, in order
    replayed = numbers(stream, dedupe=dedupe)
    diff = [{"window": w, "store": s, "stored": stored[w][s], "replayed": replayed[w][s]}
            for w in stored for s in stored[w] if stored[w][s] != replayed[w][s]]
    return ReplayReport(windows=WINDOWS, equal=not diff, differences=diff)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-dedupe", action="store_true")
    print(run(dedupe=not ap.parse_args().no_dedupe).line())
