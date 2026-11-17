"""The gate before a merge: read an Inspect log and fail when the accuracy is below the threshold.

Inspect itself does not fail on a low score; this step does. Give it a log file, or a folder to take the newest
log from.

    uv run python gate.py output/eval-stub --threshold 1.0
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from inspect_ai.log import read_eval_log


def accuracy_of(path: Path) -> float:
    if path.is_dir():
        logs = sorted(p for p in path.iterdir() if p.suffix in (".json", ".eval"))
        if not logs:
            raise SystemExit(f"no Inspect log in {path}")
        path = logs[-1]                                  # log names start with the time of the run
    log = read_eval_log(str(path))
    if log.status != "success" or not log.results:
        raise SystemExit(f"the eval did not finish: status {log.status}")
    return log.results.scores[0].metrics["accuracy"].value


def passes(path: Path, threshold: float) -> tuple[bool, float]:
    acc = accuracy_of(path)
    return acc >= threshold, acc


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("log", type=Path)
    ap.add_argument("--threshold", type=float, default=1.0)
    a = ap.parse_args()
    ok, acc = passes(a.log, a.threshold)
    print(f"accuracy {acc:.2f}, threshold {a.threshold:.2f}: {'pass' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)
