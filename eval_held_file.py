"""Eval of the agent for a held file: does it name the cause planted in each case?

An Inspect task with one sample per planted case. The solver runs the whole path of half_file_agent.triage (code
first, the agent, verify); the scorer compares the named cause with the planted one in code: no model judges.
The agent's own model is the stub by default and the course LLM gateway on opt-in; Inspect's model is the
built-in mock, which is never called.

    uv run python eval_held_file.py                    # stub model, writes an Inspect log under output/eval-stub/
    L19_LIVE=1 uv run python eval_held_file.py         # course LLM gateway, at most 16 model calls (RUN.md)
    uv run python gate.py output/eval-stub             # fails when the accuracy is below the threshold
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from inspect_ai import Task, eval as inspect_eval, task
from inspect_ai.dataset import Sample
from inspect_ai.scorer import CORRECT, INCORRECT, Score, Target, accuracy, scorer
from inspect_ai.solver import Generate, TaskState, solver

import half_file_agent
from local import LocalLake, object_key

ROOT = Path(__file__).parent
# case -> (store whose file is held, the planted cause)
CASES = {"half-file": ("centre", "partial_file"), "category-missing": ("north", "category_missing")}
LIVE_BUDGET = 16                   # model calls for one live run: 2 cases, at most 8 each
PER_CASE = LIVE_BUDGET // len(CASES)


def run_case(case: str, store: str, kind: str, model_id: str) -> dict:
    """The whole path for the held file of one case, with a new model object (the stub or the gateway)."""
    from models import DEFAULT_MODEL, gateway_model

    lake = LocalLake(case)
    record = next(e["Records"][0] for e in lake.s3_events() if f"{store}-export" in e["Records"][0]["s3"]["object"]["key"])
    model = half_file_agent.stub_model(kind) if kind.startswith("stub") else gateway_model(model_id or DEFAULT_MODEL)
    trace: list = []
    result = half_file_agent.triage(object_key(record), record["eventTime"][:19] + "Z", lake, model,
                                    caps=(PER_CASE, half_file_agent.MAX_TOOL_CALLS, half_file_agent.DEADLINE_S), trace=trace)
    return {"result": result.model_dump(), "trace": trace}


@solver
def held_file_triage(kind: str, model_id: str):
    async def solve(state: TaskState, generate: Generate) -> TaskState:
        out = await asyncio.to_thread(run_case, state.metadata["case"], state.metadata["store"], kind, model_id)
        state.metadata["run"] = out
        state.output.completion = json.dumps(out["result"])
        return state
    return solve


@scorer(metrics=[accuracy()])
def cause_in_code():
    """Correct when the named cause is the planted one. `verified` (verify's answer) is kept beside it."""
    async def score(state: TaskState, target: Target) -> Score:
        r = state.metadata["run"]["result"]
        return Score(value=CORRECT if r["cause"] == target.text else INCORRECT, answer=str(r["cause"]),
                     explanation=f"verified by code: {r['verified']}; fix: {r['fix']}; {r['model_calls']} model calls, "
                                 f"{r['tool_calls']} tool calls; stopped: {r['stopped'] or 'no'}",
                     metadata={"verified": r["verified"], "model_calls": r["model_calls"], "tool_calls": r["tool_calls"]})
    return score


@task
def held_file_causes(kind: str = "stub", model_id: str = "") -> Task:
    samples = [Sample(id=case, input=f"held file of store {store} in case {case}", target=cause,
                      metadata={"case": case, "store": store}) for case, (store, cause) in CASES.items()]
    return Task(dataset=samples, solver=held_file_triage(kind, model_id), scorer=cause_in_code())


def run(live: bool = False, kind: str = "stub", model_id: str = "", log_dir: Path | None = None):
    """Run the eval once and return Inspect's log. Samples run one after another (max_samples=1)."""
    log_dir = log_dir or ROOT / "output" / ("eval-live" if live else "eval-stub")
    (log,) = inspect_eval(held_file_causes("gateway" if live else kind, model_id), model="mockllm/model",
                          log_dir=str(log_dir), log_format="json", display="none", max_samples=1)
    return log


if __name__ == "__main__":
    live = os.environ.get("L19_LIVE") == "1"
    log = run(live=live, model_id=os.environ.get("L19_MODEL", ""))
    calls = 0
    for s in log.samples:
        sc = s.scores["cause_in_code"]
        calls += sc.metadata["model_calls"]
        print(f"{s.id:17} planted={s.target:17} named={sc.answer:17} {'correct' if sc.value == CORRECT else 'wrong':8} {sc.explanation}")
    print(f"accuracy {log.results.scores[0].metrics['accuracy'].value:.2f}; model calls {calls}"
          f"{f' of {LIVE_BUDGET}' if live else ' (stub)'}; log {Path(log.location).relative_to(ROOT)}")
