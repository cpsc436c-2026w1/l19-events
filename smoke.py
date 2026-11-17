"""One capped live run on the course LLM gateway: the half-file agent, then the column drafter.

At most BUDGET model calls in all: the agent's cap is 8 and the drafter makes at most 3. The outputs go to
output/; the token is read from the environment and is not written anywhere.

    COURSE_MCP_TOKEN=... uv run python smoke.py            # RUN.md
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import column_drafter
import half_file_agent
from local import LocalCatalog, LocalLake, object_key
from models import DEFAULT_MODEL, gateway_chat, gateway_model

BUDGET = 12
OUT = Path(__file__).parent / "output"

if __name__ == "__main__":
    model_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MODEL
    OUT.mkdir(exist_ok=True)
    used, report = 0, {"model": model_id, "budget_model_calls": BUDGET}

    lake = LocalLake("half-file")
    record = next(e["Records"][0] for e in lake.s3_events() if "centre-export" in e["Records"][0]["s3"]["object"]["key"])
    trace, t0 = [], time.monotonic()
    result = half_file_agent.triage(object_key(record), record["eventTime"][:19] + "Z", lake, gateway_model(model_id),
                                    caps=(8, 6, 900), trace=trace)
    used += result.model_calls
    report["half_file"] = {"seconds": round(time.monotonic() - t0), "result": result.model_dump(), "trace": trace}
    (OUT / "smoke.json").write_text(json.dumps(report, indent=1) + "\n")
    print(f"half-file: cause={result.cause} verified={result.verified} fix={result.fix} "
          f"model_calls={result.model_calls} tool_calls={result.tool_calls} stopped={result.stopped!r}")

    calls, chat = [], gateway_chat(model_id)

    def counted(system: str, user: str) -> str:
        if used + len(calls) >= BUDGET:
            raise RuntimeError(f"budget of {BUDGET} model calls")
        reply = chat(system, user)
        calls.append({"user": user, "reply": reply[-1500:]})
        return reply

    column_drafter.CATALOG, column_drafter.CHAT, column_drafter.PENDING = LocalCatalog("new-column"), counted, OUT / "pending"
    t0 = time.monotonic()
    drafts = [d for e in column_drafter.CATALOG.events() for d in column_drafter.handler(e)]
    for d in drafts:
        d["pending_file"] = Path(d["pending_file"]).relative_to(OUT.parent).as_posix()
    used += len(calls)
    report["new_column"] = {"seconds": round(time.monotonic() - t0), "drafts": drafts, "calls": calls}
    report["model_calls_used"] = used
    (OUT / "smoke.json").write_text(json.dumps(report, indent=1) + "\n")
    for d in drafts:
        print(f"new-column: status={d['status']} attempts={d['attempts']} description={d['description']!r}")
    print(f"model calls used: {used} of {BUDGET}")
