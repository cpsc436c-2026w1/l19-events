"""The code handlers, the half-file agent and the column drafter, on the generated events. Stub models only."""
import json
from pathlib import Path

import pytest

import batch_check
import catalog_watch
import column_drafter
import half_file_agent
import stream_window
from local import EVENTS, LocalCatalog, LocalLake
from results import CatalogResult, Draft, FileVerdict, TriageResult

# ---- stream_window ------------------------------------------------------------------------------------------


@pytest.fixture()
def stream():
    stream_window.SINK = stream_window.Sink()
    records = [json.loads(line) for line in (EVENTS / "price-updates" / "records.jsonl").read_text().splitlines()]
    truth = json.loads((EVENTS / "price-updates" / "truth.json").read_text())
    stream_window.run_local(records)
    return stream_window.SINK, truth, records


def test_window_numbers_equal_the_truth(stream):
    sink, truth, _ = stream
    final = stream_window.final_numbers(sink, "2026-10-31T09:00:00Z", 12)
    for w in truth["windows"]:
        assert final[w["start"]] == w["correct"], w["start"]


def test_redelivered_late_eur_burst_quiet(stream):
    sink, truth, records = stream
    planted = truth["planted_event_ids"]
    assert sum(w.count for w in sink.windows) + len(sink.corrections) + len(sink.rejected) == truth["records_in_stream"]
    assert len(records) == truth["records_in_stream"] + len(planted["redelivered-update"])     # 4 delivered twice
    assert sorted(sink.corrections) == sorted(planted["late-update"])
    assert {c.window_start for c in sink.corrections.values()} == {"2026-10-31T09:10:00Z"}
    assert list(sink.rejected) == planted["eur-update"] and sink.rejected[planted["eur-update"][0]].currency == "EUR"
    assert [(w.store, w.window_start[11:16]) for w in sink.windows if w.burst] == [("north", "09:20")]
    final = stream_window.final_numbers(sink, "2026-10-31T09:00:00Z", 12)
    quiet = [s[11:16] for s, per in final.items() if all(v["count"] == 0 for v in per.values())]
    assert quiet == ["09:40", "09:45"]


def test_window_handler_follows_the_lambda_contract():
    stream_window.SINK = stream_window.Sink()
    event = json.loads((EVENTS / "price-updates" / "window-event.json").read_text())
    out = stream_window.handler(event)
    assert set(out) == {"state", "batchItemFailures"} and out["batchItemFailures"] == []
    again = stream_window.handler({**event, "state": out["state"]})         # the same batch, retried
    assert again["state"] == out["state"]
    assert len(json.dumps(out["state"])) < 1_000_000                       # Lambda's limit for the state: 1 MB


# ---- batch_check --------------------------------------------------------------------------------------------

EXPECTED = {"normal-day": {"north": "pass", "centre": "pass", "online": "pass"},
            "duplicate-rows": {"north": "pass", "centre": "hold", "online": "pass"},
            "missing-store": {"north": "pass", "centre": "pass"},
            "not-converted": {"north": "pass", "centre": "pass", "online": "hold"},
            "half-file": {"north": "pass", "centre": "unsettled", "online": "pass"},
            "late-file": {"north": "pass", "centre": "rerun_needed", "online": "pass"}}
CAUSE = {"duplicate-rows": "duplicate_rows", "not-converted": "currency_not_converted", "late-file": "late_arrival"}


@pytest.mark.parametrize("case", list(EXPECTED))
def test_batch_check_verdicts(case):
    batch_check.LAKE = LocalLake(case)
    verdicts = [FileVerdict(**v) for e in batch_check.LAKE.s3_events() for v in batch_check.handler(e)]
    assert {v.store: v.verdict for v in verdicts} == EXPECTED[case]
    for v in verdicts:
        if v.verdict == "pass":
            assert (v.cause, v.fix) == ("no_fault", "none")
        elif v.verdict == "unsettled":
            assert v.cause is None and v.fix is None and v.reasons
        else:
            assert v.cause == CAUSE[case] and v.reasons
    late = [v for v in verdicts if v.verdict == "rerun_needed"]
    assert all(v.needs_approval and v.fix == "rerun_partition" for v in late)
    missing = batch_check.deadline_handler({"day": "2026-10-31", "time": "2026-10-31T01:55:00Z"})
    absent = {"missing-store": ["online"], "late-file": ["centre"]}.get(case, [])   # at 01:55 a late file is absent too
    assert missing["missing_stores"] == absent and missing["fix"] == ("ask_upstream" if absent else None)
    early = batch_check.deadline_handler({"day": "2026-10-31", "time": "2026-10-31T01:40:30Z"})
    assert "online" in early["missing_stores"] and "north" not in early["missing_stores"]


# ---- catalog_watch ------------------------------------------------------------------------------------------

def _watch(case):
    catalog_watch.CATALOG = LocalCatalog(case)
    return [CatalogResult(**r) for e in catalog_watch.CATALOG.events() for r in catalog_watch.handler(e)]


@pytest.mark.parametrize("case,actions,alert", [
    ("new-partition", ["record"], False), ("new-table", ["record"], False),
    ("new-column", ["record", "draft_description"], False), ("type-change", ["record", "block_job"], True),
    ("renamed-column", ["record", "block_job"], True), ("dropped-column", ["record", "alert_owners"], True)])
def test_catalog_watch_actions(case, actions, alert):
    (r,) = _watch(case)
    assert (r.actions, r.alert) == (actions, alert)
    if case == "new-table":
        assert len(r.lacking_description) == 4
    if case == "renamed-column":
        assert (r.removed, r.added) == (["price"], ["unit_price"])


def test_catalog_watch_repeated_event_alerts_once():
    results = _watch("repeated-event")
    assert [r.actions[0] for r in results] == ["record", "none", "none"] and sum(r.alert for r in results) == 1
    assert catalog_watch.CATALOG.stored["prices_raw"]["Table"]["VersionId"] == "8"
    assert results[0].added == ["on_promotion"] and results[0].retyped == ["price"]


def test_catalog_watch_daily_check_finds_a_change_without_an_event():
    assert _watch("no-event") == []
    (r,) = [CatalogResult(**x) for x in catalog_watch.daily_handler({})]
    assert r.removed == ["currency"] and "block_job" in r.actions and r.alert
    assert catalog_watch.daily_handler({})[0]["actions"] == ["none"]        # the next day: nothing new


# ---- the half-file agent ------------------------------------------------------------------------------------

def _triage(case, mode="stub", caps=None, store="centre"):
    lake = LocalLake(case)
    event = next(e for e in lake.s3_events() if f"{store}-export" in e["Records"][0]["s3"]["object"]["key"])
    rec, trace = event["Records"][0], []
    r = half_file_agent.triage(half_file_agent.object_key(rec), rec["eventTime"][:19] + "Z", lake,
                               half_file_agent.stub_model(mode), caps, trace)
    return r, trace


def test_agent_names_the_partial_file_and_code_confirms_it():
    r, trace = _triage("half-file")
    assert (r.decided_by, r.cause, r.verified, r.fix) == ("agent", "partial_file", True, "ask_upstream")
    assert [t["tool"] for t in trace if "tool" in t] == ["summarize_file", "previous_days", "export_log"]
    assert r.model_calls == 4 and r.tool_calls == 3 and not r.stopped and r.evidence


@pytest.mark.parametrize("case,store", [("normal-day", "centre"), ("duplicate-rows", "centre"),
                                        ("not-converted", "online"), ("late-file", "centre")])
def test_agent_does_not_run_when_code_settles_the_file(case, store):
    r, _ = _triage(case, "stub-runaway", store=store)       # a model that would never stop is never called
    assert r.decided_by == "code" and r.model_calls == 0 and r.tool_calls == 0 and r.verified


def test_a_cause_the_data_does_not_show_is_refused():
    r, _ = _triage("half-file", "stub-wrong")
    assert (r.cause, r.verified, r.fix) == ("late_arrival", False, "escalate")


def test_runaway_model_is_stopped_by_the_caps():
    r, _ = _triage("half-file", "stub-runaway", caps=(5, 4, 60))
    assert r.model_calls <= 5 and r.tool_calls == 4 and r.stopped
    assert (r.cause, r.verified, r.fix) == (None, False, "escalate")


def test_agent_tools_are_read_only_and_take_no_arguments():
    lake = LocalLake("half-file")
    before = {p: p.read_bytes() for p in lake.folder.rglob("*") if p.is_file()}
    tools = half_file_agent.make_tools(lake, "prices_raw/day=2026-10-31/centre-export.csv", "centre")
    assert [t.tool_name for t in tools] == ["summarize_file", "previous_days", "export_log"]
    for t in tools:
        assert t.tool_spec["inputSchema"]["json"].get("properties", {}) == {}
        assert len(json.dumps(t())) < 3000                    # small results
    assert before == {p: p.read_bytes() for p in lake.folder.rglob("*") if p.is_file()}


def test_agent_lambda_entry_point():
    half_file_agent.LAKE, half_file_agent.MODEL = LocalLake("half-file"), half_file_agent.stub_model()
    out = [TriageResult(**r) for e in half_file_agent.LAKE.s3_events() for r in half_file_agent.handler(e)]
    assert {r.store: r.decided_by for r in out} == {"north": "code", "centre": "agent", "online": "code"}


# ---- the column drafter -------------------------------------------------------------------------------------

SAMPLES = [True, False, False]


def test_validator():
    ok = {"description": "Boolean flag; true and false both appear in the samples.", "sample_values_used": ["true"]}
    assert column_drafter.validate(ok, "boolean", SAMPLES) == []
    bad = lambda **kw: column_drafter.validate({**ok, **kw}, "boolean", SAMPLES)   # noqa: E731
    assert any("limit" in p for p in bad(description="boolean " + "x" * 260))
    assert any("type" in p for p in bad(description="A flag for promotions."))
    assert any("not in the samples" in p for p in bad(sample_values_used=["maybe"]))
    assert any("numbers" in p for p in bad(description="boolean flag, true for 20 % of rows"))
    assert any("claims more" in p for p in bad(description="boolean flag that is always true on promotion"))
    assert column_drafter.validate({}, "boolean", SAMPLES)


def _draft(mode, tmp_path):
    column_drafter.CATALOG, column_drafter.PENDING = LocalCatalog("new-column"), tmp_path
    calls = []
    chat = column_drafter.stub_chat(mode)
    column_drafter.CHAT = lambda s, u: calls.append(u) or chat(s, u)
    (out,) = [d for e in column_drafter.CATALOG.events() for d in column_drafter.handler(e)]
    return Draft(**{k: v for k, v in out.items() if k != "pending_file"}), calls, Path(out["pending_file"])


def test_drafter_retries_with_the_validator_feedback_and_waits_for_a_person(tmp_path):
    d, calls, pending = _draft("stub", tmp_path)
    assert (d.status, d.attempts, d.column, d.type) == ("draft", 2, "on_promotion", "boolean")
    assert "rejected" in calls[1] and "sample_values_used names values" in calls[1]
    assert len(d.description) <= 255 and "boolean" in d.description.lower()
    assert json.loads(pending.read_text())["status"] == "draft" and pending.parent == tmp_path
    after = column_drafter.CATALOG.get_table("", "prices_raw")["Table"]["StorageDescriptor"]["Columns"]
    assert next(c for c in after if c["Name"] == "on_promotion")["Comment"] == ""    # the catalog is not written


def test_drafter_stops_after_two_retries(tmp_path):
    d, calls, pending = _draft("stub-stubborn", tmp_path)
    assert (d.status, d.attempts, len(calls), d.description) == ("cannot_describe", 3, 3, "") and d.problems
    assert json.loads(pending.read_text())["status"] == "cannot_describe"


@pytest.mark.parametrize("case", ["type-change", "renamed-column", "dropped-column", "new-partition"])
def test_drafter_makes_no_model_call_without_a_new_undescribed_column(case, tmp_path):
    column_drafter.CATALOG, column_drafter.PENDING = LocalCatalog(case), tmp_path
    column_drafter.CHAT = lambda s, u: pytest.fail("no model call expected")
    assert [d for e in column_drafter.CATALOG.events() for d in column_drafter.handler(e)] == []
