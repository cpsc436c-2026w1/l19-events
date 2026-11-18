"""The studio's switch: L19_DEDUPE=off turns off the eventID dedupe of the window handler for one run."""
import json
import os
import subprocess
from pathlib import Path

import pytest

import replay
import stream_window
from local import EVENTS

ROOT = Path(__file__).parent.parent
RECORDS = [json.loads(line) for line in (EVENTS / "price-updates" / "records.jsonl").read_text().splitlines()]
W0925 = "2026-10-31T09:25:00Z"


def _online_0925() -> int:
    stream_window.SINK = stream_window.Sink()
    stream_window.run_local(RECORDS)
    return stream_window.final_numbers(stream_window.SINK, "2026-10-31T09:00:00Z", 12)[W0925]["online"]["count"]


def test_dedupe_is_on_by_default(monkeypatch):
    monkeypatch.delenv("L19_DEDUPE", raising=False)
    assert _online_0925() == 4


def test_dedupe_off_counts_the_redelivered_updates_again(monkeypatch):
    monkeypatch.setenv("L19_DEDUPE", "off")
    assert _online_0925() == 7                       # 3 of the 4 redelivered updates are of store online
    monkeypatch.setenv("L19_DEDUPE", "on")
    assert _online_0925() == 4


def test_an_explicit_argument_wins_over_the_switch(monkeypatch):
    monkeypatch.setenv("L19_DEDUPE", "off")
    assert replay.run(dedupe=True).equal


def test_another_value_of_the_switch_is_refused(monkeypatch):
    monkeypatch.setenv("L19_DEDUPE", "no")
    with pytest.raises(ValueError, match="L19_DEDUPE"):
        _online_0925()


def _behave(tags: str, dedupe: str | None) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if k != "L19_DEDUPE"} | ({"L19_DEDUPE": dedupe} if dedupe else {})
    return subprocess.run(["behave", "--no-capture", f"--tags={tags}"], cwd=ROOT, env=env,
                          capture_output=True, text=True)


def test_the_rf010_scenario_passes_with_dedupe_and_fails_without_it():
    on = _behave("@RF010", None)
    assert on.returncode == 0, on.stdout
    off = _behave("@RF010", "off")
    assert off.returncode != 0
    assert "updates counted for store centre between 09:25 and 09:30: expected 1, actual 2" in off.stdout
    assert "updates counted for store online between 09:25 and 09:30: expected 4, actual 7" in off.stdout
    assert "window" not in off.stdout.split("ASSERT FAILED")[1].splitlines()[0]


@pytest.mark.parametrize("tag", ["@RF020", "@RF030"])
def test_the_quiet_store_and_the_late_update_scenarios_pass(tag):
    run = _behave(tag, None)
    assert run.returncode == 0 and "1 scenario passed" in run.stdout, run.stdout


def test_a_store_with_updates_in_the_quiet_period_fails_with_a_plain_message(tmp_path):
    import importlib.util
    import types
    spec = importlib.util.spec_from_file_location("l19_steps", ROOT / "features" / "steps" / "steps.py")
    S = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(S)
    final = {"2026-10-31T09:40:00Z": {"north": {"count": 2, "avg_price": 6.95}, "centre": {"count": 0, "avg_price": None}}}
    with pytest.raises(AssertionError, match="store north between 09:40 and 09:45: expected no update and no price, "
                                             "actual 2 updates and the price 6.95"):
        S.step_quiet_shown(types.SimpleNamespace(final=final), "09:40", "09:45")
