"""Steps for the three job stories. The Given steps take their data from the simulator's files in events/; the
When steps call the Lambda-shaped handlers; the Then steps compare typed results. No AWS, no model: the agent and
the drafter run on stub models, and the eval runs live only when L19_LIVE=1."""
import json
import os
from pathlib import Path

from behave import given, then, when

import batch_check
import catalog_watch
import column_drafter
import eval_held_file
import gate
import half_file_agent
import stream_window
from local import EVENTS, LocalCatalog, LocalLake, object_key

DAY = "2026-10-31"
FIRST = f"{DAY}T09:00:00Z"

# ---- JS01: price updates ------------------------------------------------------------------------------------


@given("the simulator delivers one hour of price updates to the window function")
def step_stream(context):
    folder = EVENTS / "price-updates"
    context.records = [json.loads(line) for line in (folder / "records.jsonl").read_text().splitlines()]
    context.truth = json.loads((folder / "truth.json").read_text())
    stream_window.SINK = context.sink = stream_window.Sink()
    stream_window.run_local(context.records)
    context.final = stream_window.final_numbers(context.sink, FIRST, 12)


def _planted(context, name):
    return context.truth["planted_event_ids"][name]


@given("the simulator delivers {n:d} of the updates a second time")
def step_redelivered(context, n):
    ids = [r["eventID"] for r in context.records]
    assert sorted(i for i in set(ids) if ids.count(i) == 2) == sorted(_planted(context, "redelivered-update"))
    assert len(_planted(context, "redelivered-update")) == n


@given("the simulator holds the updates of store centre from 09:10 to 09:16")
def step_late(context):
    assert _planted(context, "late-update")


@given("the simulator sends one update of store online in EUR")
def step_eur(context):
    assert len(_planted(context, "eur-update")) == 1


@given("the simulator sends {n:d} updates of store north in two minutes from 09:20")
def step_burst(context, n):
    assert len(_planted(context, "store-burst")) == n


@given("the simulator does not send an update from 09:40 to 09:50")
def step_quiet(context):
    assert context.truth["quiet"] == {"from": f"{DAY}T09:40:00Z", "to": f"{DAY}T09:50:00Z"}


def _cell(context, hhmm, store):
    return context.final[f"{DAY}T{hhmm}:00Z"][store]


@then("window {hhmm} has {n:d} updates with an average of {avg:f} for store {store}")
def step_window_avg(context, hhmm, n, avg, store):
    assert _cell(context, hhmm, store) == {"count": n, "avg_price": avg}, _cell(context, hhmm, store)


@then("window {hhmm} has {n:d} updates for store {store}")
def step_window_count(context, hhmm, n, store):
    assert _cell(context, hhmm, store)["count"] == n, _cell(context, hhmm, store)


@then("window {hhmm} has the simulator's truth count for every store")
def step_window_truth(context, hhmm):
    start = f"{DAY}T{hhmm}:00Z"
    correct = next(w["correct"] for w in context.truth["windows"] if w["start"] == start)
    wrong = [f"store {store}: expected count {want['count']} (the simulator's truth), "
             f"actual count {context.final[start][store]['count']}"
             for store, want in correct.items() if context.final[start][store]["count"] != want["count"]]
    assert not wrong, f"window {hhmm}, " + "; ".join(wrong)


@then("every window has the count and the average of the simulator's truth")
def step_truth(context):
    for w in context.truth["windows"]:
        assert context.final[w["start"]] == w["correct"], w["start"]


@then("the number of updates stored equals the number in the stream")
def step_once(context):
    s = context.sink
    assert sum(w.count for w in s.windows) + len(s.corrections) + len(s.rejected) == context.truth["records_in_stream"]


@then("{n:d} updates are stored as corrections for window {hhmm}")
def step_corrections(context, n, hhmm):
    got = [c for c in context.sink.corrections.values() if c.window_start == f"{DAY}T{hhmm}:00Z"]
    assert len(got) == n == len(context.sink.corrections)


@then("{n:d} update is in the rejected list with currency {currency}")
def step_rejected(context, n, currency):
    assert [r.currency for r in context.sink.rejected.values()] == [currency] * n


@then("the only window flagged as a burst is {hhmm} for store {store}")
def step_flag(context, hhmm, store):
    assert [(w.store, w.window_start[11:16]) for w in context.sink.windows if w.burst] == [(store, hhmm)]


@then("windows {a} and {b} are reported with a count of 0 for every store")
def step_zero(context, a, b):
    quiet = [s[11:16] for s, per in context.final.items() if all(v["count"] == 0 for v in per.values())]
    assert quiet == [a, b], quiet


# ---- JS02: export files -------------------------------------------------------------------------------------

@given('the simulator delivers the export files of the night "{night}"')
def step_night(context, night):
    context.lake = batch_check.LAKE = half_file_agent.LAKE = LocalLake(night)
    context.model_calls = 0


@when("the file check runs on each new file")
def step_check(context):
    context.verdicts = {v["store"]: v for e in context.lake.s3_events() for v in batch_check.handler(e)}


@when("the check for missing files runs at 01:55")
def step_deadline(context):
    context.missing = batch_check.deadline_handler({"day": DAY, "time": f"{DAY}T01:55:00Z"})


@then("{passed:d} files pass and {held:d} files are held")
def step_pass_held(context, passed, held):
    v = [x["verdict"] for x in context.verdicts.values()]
    assert (v.count("pass"), sum(x in ("hold", "unsettled") for x in v)) == (passed, held), v


@then("{passed:d} files pass")
def step_pass(context, passed):
    assert [x["verdict"] for x in context.verdicts.values()].count("pass") == passed


@then("the agent makes 0 model calls")
def step_no_model(context):
    assert all(v["verdict"] != "unsettled" for v in context.verdicts.values())      # only an unsettled file reaches it


@then('the file of store {store} has the verdict "{verdict}" and the cause "{cause}"')
def step_verdict(context, store, verdict, cause):
    v = context.verdicts[store]
    assert (v["verdict"], v["cause"]) == (verdict, cause), v


@then("the check reports {n:d} repeated ids for store {store}")
def step_repeated(context, n, store):
    assert context.verdicts[store]["facts"]["repeated_obs_ids"] == n


@then('the stores named as missing are "{missing}"')
def step_missing(context, missing):
    assert context.missing["missing_stores"] == missing.split(), context.missing


@then('the stores named as missing are ""')
def step_none_missing(context):
    assert context.missing["missing_stores"] == [] and context.missing["fix"] is None


@then("the file of store {store} is held")
def step_held(context, store):
    assert context.verdicts[store]["verdict"] in ("hold", "unsettled"), context.verdicts[store]


@then('the fix "{fix}" waits for approval')
def step_approval(context, fix):
    waiting = [v for v in context.verdicts.values() if v["needs_approval"]]
    assert [v["fix"] for v in waiting] == [fix]


@given('the agent\'s model is the stub "{mode}"')
def step_stub(context, mode):
    context.model = half_file_agent.stub_model(mode)


@when("the agent triages the held file of store {store}")
def step_triage(context, store):
    rec = next(e["Records"][0] for e in context.lake.s3_events() if f"{store}-export" in e["Records"][0]["s3"]["object"]["key"])
    context.trace = []
    context.result = half_file_agent.triage(object_key(rec), rec["eventTime"][:19] + "Z", context.lake, context.model,
                                            trace=context.trace)


@then('the cause is "{cause}" and code confirms it')
def step_cause(context, cause):
    r = context.result
    assert (r.cause, r.verified, r.decided_by) == (cause, True, "agent"), r


@then('the cause "{cause}" is not confirmed by code')
def step_refused(context, cause):
    assert (context.result.cause, context.result.verified) == (cause, False)


@then('the proposed fix is "{fix}"')
def step_fix(context, fix):
    assert context.result.fix == fix, context.result


@then("the agent made at most {m:d} model calls and {t:d} tool calls")
def step_caps(context, m, t):
    assert context.result.model_calls <= m and context.result.tool_calls <= t, context.result


@then("the run was stopped by the app")
def step_stopped(context):
    assert context.result.stopped.startswith("cap of"), context.result.stopped


@then("the first tool call was refused with an error")
def step_tool_error(context):
    assert "error" in context.trace[0]["result"], context.trace[0]


# ---- JS02: the eval -----------------------------------------------------------------------------------------

@given("the eval of the agent on its planted cases")
def step_eval(context):
    context.live = os.environ.get("L19_LIVE") == "1"
    # stub: every planted cause must be named. live: the threshold is a starting value, to be set from repeated
    # runs of unchanged code (L19_LIVE_THRESHOLD).
    context.threshold = float(os.environ.get("L19_LIVE_THRESHOLD", "0.5")) if context.live else 1.0


@when("the eval runs")
def step_eval_run(context):
    log_dir = Path(context.tmp.name) / "eval" if not context.live else None
    context.log = eval_held_file.run(live=context.live, model_id=os.environ.get("L19_MODEL", ""), log_dir=log_dir)


@then("the gate passes at the threshold")
def step_gate(context):
    ok, acc = gate.passes(Path(context.log.location), context.threshold)
    assert ok, f"accuracy {acc} is below {context.threshold}"


@then("every planted case has a score in the log")
def step_scores(context):
    assert sorted(s.id for s in context.log.samples) == sorted(eval_held_file.CASES)
    assert all("cause_in_code" in s.scores for s in context.log.samples)


# ---- JS03: catalog changes ----------------------------------------------------------------------------------

@given('the simulator delivers the catalog events of the case "{case}"')
def step_catalog(context, case):
    context.catalog = catalog_watch.CATALOG = column_drafter.CATALOG = LocalCatalog(case)
    context.results = []


@when("the catalog watch runs on each event")
def step_watch(context):
    context.refused = None
    try:
        context.results = [r for e in context.catalog.events() for r in catalog_watch.handler(e)]
    except ValueError as e:
        context.refused = str(e)


@when("the daily check runs")
def step_daily(context):
    context.results += catalog_watch.daily_handler({})


@then('the table "{table}" is recorded')
def step_recorded(context, table):
    assert [(r["table"], r["actions"]) for r in context.results] == [(table, ["record"])], context.results


@then("{n:d} alerts are raised")
@then("{n:d} alert is raised")
def step_alerts(context, n):
    assert sum(r["alert"] for r in context.results) == n, context.results


@then('the actions include "{action}"')
def step_action(context, action):
    assert any(action in r["actions"] for r in context.results), context.results


@then('the actions do not include "{action}"')
def step_no_action(context, action):
    assert not any(action in r["actions"] for r in context.results), context.results


@then('the stored version of "{table}" is {version:d}')
def step_version(context, table, version):
    assert context.catalog.stored[table]["Table"]["VersionId"] == str(version)


@then('the removed columns are "{columns}"')
def step_removed(context, columns):
    assert [c for r in context.results for c in r["removed"]] == columns.split()


@given('the drafter\'s model is the stub "{mode}"')
def step_drafter_stub(context, mode):
    chat, context.calls = column_drafter.stub_chat(mode), []
    column_drafter.CHAT = lambda system, user: context.calls.append(user) or chat(system, user)
    column_drafter.PENDING = Path(context.tmp.name) / "pending"


@when("the drafter runs on each event")
def step_draft(context):
    context.drafts = [d for e in context.catalog.events() for d in column_drafter.handler(e)]


@then('a draft for column "{column}" is in the pending folder with status "{status}"')
def step_pending(context, column, status):
    (d,) = [x for x in context.drafts if x["column"] == column]
    saved = json.loads(Path(d["pending_file"]).read_text())
    assert saved["status"] == status and Path(d["pending_file"]).parent == column_drafter.PENDING


@then('the draft has at most {n:d} characters and names the type "{column_type}"')
def step_draft_text(context, n, column_type):
    text = context.drafts[0]["description"]
    assert 0 < len(text) <= n and column_type in text.lower(), text


@then('the column "{column}" does not have a description in the catalog')
def step_catalog_unwritten(context, column):
    table = context.catalog.get_table("", context.drafts[0]["table"])
    assert next(c for c in table["Table"]["StorageDescriptor"]["Columns"] if c["Name"] == column)["Comment"] == ""


@then("the drafter made {n:d} model calls")
def step_draft_calls(context, n):
    assert len(context.calls) == n, len(context.calls)


@then("the second user message carries the check's feedback")
def step_feedback(context):
    assert "It was rejected:" in context.calls[1] and "rejected" not in context.calls[0]


# ---- part 1: input models, failing batch, replay, load ------------------------------------------------------

import asyncio  # noqa: E402

import failing_batch  # noqa: E402
import load  # noqa: E402
import replay  # noqa: E402

FAULTS = {"a missing price": 0, "a price as text": 1, "an unknown store": 2}


@given("the simulator plants an update with {fault}")
def step_invalid(context, fault):
    lines = (EVENTS / "price-updates" / "invalid-records.jsonl").read_text().splitlines()
    context.bad = json.loads(lines[FAULTS[fault]])


@when("the window function receives it")
def step_receive(context):
    stream_window.SINK = context.sink = stream_window.Sink()
    event = json.loads((EVENTS / "price-updates" / "window-event.json").read_text())
    try:
        context.out, context.raised = stream_window.handler({**event, "Records": [context.bad]}), None
    except Exception as e:
        context.raised = e


@then('the update is in the rejected list and the reason names the field "{field}"')
def step_rejected_field(context, field):
    (r,) = context.sink.rejected.values()
    assert r.event_id == context.bad["eventID"] and r.reason.startswith(f"{field}: "), r


@then("the window function does not fail")
def step_no_raise(context):
    assert context.raised is None and context.out["batchItemFailures"] == []


@given("the simulator plants a batch of 8 updates whose sixth lacks a price")
def step_failing_batch(context):
    event = json.loads(failing_batch.EVENT.read_text())
    assert len(event["Records"]) == 8 and "price" not in event["Records"][5]["dynamodb"]["NewImage"]
    context.validate = True


@given("the window function does not check its input")
def step_unvalidated(context):
    context.validate = False


@when("Lambda delivers the batch, splits it on an error and sends a failed record aside")
def step_bisect(context):
    context.report = failing_batch.run(validate=context.validate)


@then("{n:d} updates are applied once")
def step_applied(context, n):
    assert context.report.applied == n, context.report


@then("{n:d} record is in the failure list, with its sequence number and without its content")
def step_failure_list(context, n):
    f = context.report.failures
    assert len(f) == n and f[0]["DDBStreamBatchInfo"]["startSequenceNumber"].isdigit() and "NewImage" not in json.dumps(f)


@then("{n:d} records are in the failure list")
def step_failures(context, n):
    assert len(context.report.failures) == n


@then("delivering the batch again counts {n:d} updates twice")
def step_twice(context, n):
    assert context.report.counted_twice == n


@given("the window function does not skip an update it has seen")
def step_no_dedupe(context):
    context.dedupe = False


@when("the window numbers are rebuilt from the start of the stream")
def step_replay(context):
    context.replay = replay.run(dedupe=getattr(context, "dedupe", True))


@then("the replayed numbers equal the stored numbers")
def step_replay_equal(context):
    assert context.replay.equal, context.replay.differences


@then("the replayed numbers differ from the stored numbers in window {hhmm} only")
def step_replay_differs(context, hhmm):
    assert {d["window"][11:16] for d in context.replay.differences} == {hhmm}


@given('the price on line {n:d} of the file of store {store} is "{value}"')
def step_bad_row(context, n, store, value):
    key = f"prices_raw/day={DAY}/{store}-export.csv"
    lines = context.lake.get_object(key).decode().splitlines()
    cells = lines[n - 1].split(",")
    cells[4] = value
    lines[n - 1] = ",".join(cells)
    data, original = ("\n".join(lines) + "\n").encode(), context.lake.get_object
    context.lake.get_object = lambda k: data if k == key else original(k)


@then('the reason names line {n:d} and the field "{field}"')
def step_line_field(context, n, field):
    held = [v for v in context.verdicts.values() if v["verdict"] == "hold"]
    assert held[0]["reasons"][0].startswith(f"line {n}: {field}: "), held


@given('the first event has the operation "{operation}"')
def step_bad_event(context, operation):
    events = context.catalog.events()
    events[0]["detail"]["typeOfChange"] = operation
    context.catalog.events = lambda: events


@then('the watch refuses the event and names the field "{field}"')
def step_refuse_event(context, field):
    assert context.refused and field in context.refused, context.refused


@given("the simulator streams the price updates from {hhmm} for {minutes:d} minutes")
def step_load(context, hhmm, minutes):
    context.load = {"start_s": int(hhmm[3:]) * 60, "minutes": minutes, "handler_delay_s": 0.0}


@given("the handler needs {seconds:d} seconds for each update")
def step_slow(context, seconds):
    context.load["handler_delay_s"] = float(seconds)


@when("{n:d} simulated shoppers ask for latest prices during that time")
def step_users(context, n):
    context.load_report = asyncio.run(load.run(users=n, **context.load))


@then("the p99 latency of the queries is under {ms:d} ms")
def step_p99(context, ms):
    assert context.load_report.p99_ms < ms, context.load_report


@then("the largest staleness is at most {s:d} seconds")
def step_fresh(context, s):
    assert context.load_report.max_staleness_s <= s, context.load_report


@then("the largest staleness is more than {s:d} seconds")
def step_stale(context, s):
    assert context.load_report.max_staleness_s > s, context.load_report


@then("the report says the answers are within the window")
def step_report_ok(context):
    assert "within the window" in context.load_report.line()


@then("the report says the handler is behind")
def step_report_behind(context):
    assert "the handler is behind" in context.load_report.line()


# ---- JS04 R040 offline, and the @live scenarios (AWS) -------------------------------------------------------

import uuid  # noqa: E402
from datetime import datetime, timezone  # noqa: E402

from inputs import PriceUpdate  # noqa: E402


@given("an in-memory price store")
def step_store(context):
    context.store = load.LatestPrices()


@when("an update observed at {first} and then an update observed at {second} are written for one product")
def step_two_writes(context, first, second):
    def update(hhmmss, price):
        return PriceUpdate(event_id="e", event_name="MODIFY", sequence_number="1" * 21, created_at=0, product_code="p",
                           store="north", price=price, currency="CAD", update_id=hhmmss,
                           observed_at=datetime.fromisoformat(f"{DAY}T{hhmmss}+00:00"))

    async def go():
        return [await context.store.put_if_newer(update(first, 2.0)), await context.store.put_if_newer(update(second, 1.0)),
                await context.store.get_latest("p", "north")]
    *context.wins, context.item = asyncio.run(go())
    context.first = first


@then("the first write wins and the second is refused")
def step_wins(context):
    assert context.wins == [True, False]


@then("the stored price is the one observed at {hhmmss}")
def step_stored(context, hhmmss):
    assert context.item["update_id"] == hhmmss and context.item["price"] == 2.0


@given("the l19 resources are up in my AWS account")
def step_live_up(context):
    from aws import deploy
    context.deploy, context.aws = deploy, deploy.clients_for()
    account = context.aws["sts"].get_caller_identity()["Account"]
    found = deploy.existing(context.aws, account)
    missing = [k for k, v in found.items() if v is None and not k.startswith("log group")]
    assert not missing, f"run `uv run python -m aws.deploy up` first; missing: {missing}"
    context.run_id = uuid.uuid4().hex[:6]


@when("two clients race {rounds:d} rounds of conditional writes on one item")
def step_live_race(context, rounds):
    from aws import clients
    context.race = clients.run_race(lambda: context.deploy.client("dynamodb"), context.deploy.PRICES, context.run_id,
                                    rounds=rounds)


@then("every round has exactly one winner")
def step_live_one_winner(context):
    r = context.race
    assert r["rounds_with_exactly_one_winner"] == r["rounds"] and not r["other_errors"], r


@then("the final item holds the newest observed time")
def step_live_final(context):
    assert context.race["final_item"]["observed_at"] == context.race["expected_observed_at"]


@when("{users:d} clients read latest prices for {seconds:d} seconds while updates are written")
def step_live_load(context, users, seconds):
    from aws import clients
    ddb = context.deploy.client("dynamodb", max_pool_connections=max(64, users + 14))
    context.live_load = clients.run_load(ddb, context.deploy.PRICES, context.run_id, users=users, seconds=seconds)


@then("the p99 latency of the reads is under {ms:d} ms")
def step_live_p99(context, ms):
    assert context.live_load["p99_ms"] < ms, context.live_load


@then("the largest staleness is under {s:d} seconds")
def step_live_stale(context, s):
    assert context.live_load["max_staleness_ms"] < s * 1000, context.live_load


@when("{good:d} good updates and one without a price are written to the price table")
def step_live_batch(context, good):
    from aws import clients
    context.queue_url = context.aws["sqs"].get_queue_url(QueueName=context.deploy.QUEUE)["QueueUrl"]
    clients.read_failures(context.aws["sqs"], context.queue_url, wait_s=1)
    context.written = clients.write_batch(context.aws["dynamodb"], context.deploy.PRICES, context.run_id, good=good)


@when("the 5-minute window has ended")
def step_live_wait(context):
    from aws import clients
    context.batch = clients.wait_for_batch(context.aws["dynamodb"], context.aws["sqs"], context.deploy.WINDOWS,
                                           context.queue_url, context.written)


@then("{n:d} updates are counted in the window table, each once")
def step_live_counted(context, n):
    assert context.batch["observed"] and context.batch["counted"] == n, context.batch


@then("the on-failure queue holds {n:d} record for a batch of {size:d}")
def step_live_queue(context, n, size):
    f = context.batch["failures"]
    assert len(f) == n and f[0]["DDBStreamBatchInfo"]["batchSize"] == size, f


@given("the l19 agent resources are up in my AWS account")
def step_agent_up(context):
    from aws import agent, deploy
    context.agent, context.region = agent, deploy.region()
    context.aws = {**deploy.clients_for(), "s3": deploy.client("s3")}
    account = context.aws["sts"].get_caller_identity()["Account"]
    found = agent.existing(context.aws, account, context.region)
    missing = [k for k, v in found.items() if v is None and not k.startswith("log group")]
    assert not missing, f"run `uv run python -m aws.deploy agent-up` first; missing: {missing}"


@when("the two planted files are uploaded to the raw prefix")
def step_agent_run(context):
    context.agent_run = context.agent.run(context.aws, context.region)


@then("a result is written for each file within 11 minutes")
def step_agent_results(context):
    assert set(context.agent_run["results"]) == set(context.agent.CASES), context.agent_run


@then('every named cause is either confirmed by code or the fix is "escalate"')
def step_agent_checked(context):
    for r in context.agent_run["results"].values():
        assert r["verified"] or r["fix"] == "escalate", r


@then("the agent made at most {n:d} model calls in all")
def step_agent_budget(context, n):
    assert context.agent_run["model_calls"] <= n, context.agent_run
