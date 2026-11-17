"""Input models, the load run, the failing batch and the replay. No model calls."""
import asyncio
import json

import pytest
from pydantic import ValidationError

import batch_check
import catalog_watch
import failing_batch
import load
import replay
import stream_window
from inputs import BatchRow, CatalogEvent, PriceUpdate, problems
from local import EVENTS, LocalCatalog, LocalLake

UPDATES = EVENTS / "price-updates"


def _problems(model, data, **kw):
    with pytest.raises(ValidationError) as e:
        model.from_record(data) if model is PriceUpdate else model.model_validate(data, **kw)
    return problems(e.value)


# ---- input models -------------------------------------------------------------------------------------------

def test_every_generated_event_fits_its_model():
    for line in (UPDATES / "records.jsonl").read_text().splitlines():
        u = PriceUpdate.from_record(json.loads(line))
        assert u.price > 0 and u.store in ("north", "centre", "online")
    lake = LocalLake("normal-day")
    for key in ("north", "centre", "online"):
        rows = batch_check.read_rows(lake.get_object(f"prices_raw/day=2026-10-31/{key}-export.csv"))
        assert batch_check.invalid_rows(rows) == []
    for case in ("new-partition", "new-table", "new-column", "repeated-event"):
        for e in LocalCatalog(case).events():
            CatalogEvent.model_validate(e)


def test_planted_invalid_updates_fail_with_a_message_that_names_the_field():
    bad = [json.loads(line) for line in (UPDATES / "invalid-records.jsonl").read_text().splitlines()]
    got = [_problems(PriceUpdate, r) for r in bad]
    assert [len(g) for g in got] == [1, 1, 1]
    assert got[0][0] == "price: Field required"
    assert got[1][0].startswith("price: Input should be a valid number") and "four dollars" in got[1][0]
    assert got[2][0].startswith("store: Input should be 'north', 'centre' or 'online'") and "south" in got[2][0]


def test_window_function_rejects_invalid_updates_and_counts_the_rest():
    stream_window.SINK = stream_window.Sink()
    event = json.loads((UPDATES / "failing-batch.json").read_text())
    bad = [json.loads(line) for line in (UPDATES / "invalid-records.jsonl").read_text().splitlines()]
    out = stream_window.handler({**event, "Records": event["Records"] + bad[1:]})
    assert sum(s["count"] for s in out["state"]["stores"].values()) == 7
    reasons = sorted(r.reason.split(":")[0] for r in stream_window.SINK.rejected.values())
    assert reasons == ["price", "price", "store"]


def test_batch_row_and_catalog_event_name_the_field():
    row = {"obs_id": "a" * 32, "product_code": "20057237", "store": "north", "observed_on": "2026-10-31",
           "price": "3.10", "currency": "CAD", "source": "receipt:x"}
    assert BatchRow.model_validate(row).price == 3.10
    assert _problems(BatchRow, {**row, "price": "-1"})[0].startswith("price: Input should be greater than 0")
    assert _problems(BatchRow, {**row, "currency": "USD"})[0].startswith("currency:")
    assert _problems(BatchRow, {k: v for k, v in row.items() if k != "observed_on"}) == ["observed_on: Field required"]
    event = LocalCatalog("new-column").events()[0]
    broken = {**event, "detail": {**event["detail"], "typeOfChange": "RenameTable"}}
    assert any(p.startswith("detail.typeOfChange:") for p in _problems(CatalogEvent, broken))
    catalog_watch.CATALOG = LocalCatalog("new-column")
    with pytest.raises(ValueError, match="detail.typeOfChange"):
        catalog_watch.handler(broken)


def test_a_file_with_a_row_that_does_not_fit_is_held_with_line_and_field():
    lake = LocalLake("normal-day")
    key = "prices_raw/day=2026-10-31/north-export.csv"
    lines = lake.get_object(key).decode().splitlines()
    cells = lines[3].split(",")
    cells[4] = "n/a"                                                   # the price of the row on line 4
    lines[3] = ",".join(cells)
    v = batch_check.check(key, ("\n".join(lines) + "\n").encode(), "2026-10-31T01:40:10Z",
                          lake.baseline("north")["usual"], lake.job_started_at())
    assert (v.verdict, v.cause, v.fix) == ("hold", "other", "escalate")
    assert v.reasons[0].startswith("line 4: price: Input should be a valid number")


# ---- load ---------------------------------------------------------------------------------------------------

def test_staleness_stays_inside_the_window_when_the_handler_keeps_up():
    r = asyncio.run(load.run(users=50, minutes=4))
    assert r.within_window and r.max_staleness_s < 60 and r.queries > 1000 and r.updates_applied >= 120
    assert 0 < r.p50_ms <= r.p99_ms and "within the window" in r.line()


def test_a_slow_handler_gives_answers_older_than_the_window_and_the_report_says_so():
    r = asyncio.run(load.run(users=50, minutes=10, handler_delay_s=4))
    assert not r.within_window and r.max_staleness_s > load.WINDOW_S
    assert "OLDER THAN THE WINDOW" in r.line()


def test_the_store_keeps_the_newer_price_when_an_older_update_arrives_late():
    async def go():
        store = load.LatestPrices()
        a, b = load.load_updates(20 * 60, 1)[:2]
        newer = a.model_copy(update={"observed_at": b.observed_at, "product_code": "p", "store": "north", "price": 2.0})
        older = a.model_copy(update={"product_code": "p", "store": "north", "price": 1.0})
        assert older.observed_at < newer.observed_at
        return [await store.put_if_newer(newer), await store.put_if_newer(older), (await store.get_latest("p", "north"))["price"]]
    assert asyncio.run(go()) == [True, False, 2.0]


# ---- failing batch ------------------------------------------------------------------------------------------

def test_bisect_isolates_the_bad_record_and_applies_the_others_once():
    r = failing_batch.run()
    assert r.invocations == ["8:KeyError", "4:ok", "4:KeyError", "2:KeyError", "1:ok", "1:KeyError", "2:ok"]
    assert (r.applied, len(r.failures), r.counted_twice) == (7, 1, 0)
    bad = json.loads((UPDATES / "invalid-records.jsonl").read_text().splitlines()[0])
    assert r.failures[0]["startSequenceNumber"] == bad["dynamodb"]["SequenceNumber"]
    assert "NewImage" not in json.dumps(r.failures)                    # metadata about the batch, not the record


def test_with_the_input_check_the_batch_does_not_fail():
    r = failing_batch.run(validate=True)
    assert r.invocations == ["8:ok"] and (r.applied, r.failures) == (7, [])
    assert [x.reason for x in stream_window.SINK.rejected.values()] == ["price: Field required"]


# ---- replay -------------------------------------------------------------------------------------------------

def test_replay_gives_the_stored_numbers():
    r = replay.run()
    assert r.equal and r.differences == [] and "equal" in r.line()


def test_replay_shows_what_a_function_without_the_dedupe_counted_twice():
    r = replay.run(dedupe=False)
    assert not r.equal
    assert [(d["window"][11:16], d["store"], d["stored"]["count"], d["replayed"]["count"]) for d in r.differences] == [
        ("09:25", "centre", 2, 1), ("09:25", "online", 7, 4)]
