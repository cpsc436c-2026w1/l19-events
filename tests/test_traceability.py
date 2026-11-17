"""The traceability check, the eval on the stub and the gate."""
from pathlib import Path

import check_traceability as T
import eval_held_file
import gate

ROOT = Path(__file__).parent.parent
STORIES = "## JS01 A story\n| R010 | x |\n| R020 | y |\n## JS02 Another\n| R010 | z |\n"


def _feat(tags, *scenarios):
    return {"file": "f.feature", "tags": tags, "scenarios": [{"name": n, "tags": t, "runs": 1} for n, t in scenarios]}


def test_the_repository_is_traceable():
    problems, covered = T.check((ROOT / "STORIES.md").read_text(), T.features(ROOT / "features"))
    assert problems == []
    assert {s: sum(r.values()) for s, r in covered.items()} == {"JS01": 13, "JS02": 18, "JS03": 11, "JS04": 3}


def test_a_story_without_a_feature_fails():
    problems, _ = T.check(STORIES, [_feat(["JS01"], ("a", ["R010"]), ("b", ["R020"]))])
    assert problems == ["JS02: no feature is tagged @JS02"]


def test_a_requirement_without_a_scenario_fails():
    problems, _ = T.check(STORIES, [_feat(["JS01"], ("a", ["R010"])), _feat(["JS02"], ("c", ["R010"]))])
    assert problems == ["JS01 R020: no scenario is tagged @R020"]


def test_a_tag_that_names_nothing_fails():
    feats = [_feat(["JS01"], ("a", ["R010"]), ("b", ["R020"]), ("d", ["R090"])), _feat(["JS02"], ("c", ["R010"])),
             _feat(["JS07"], ("e", ["R010"])), _feat(["JS02"], ("untagged", []))]
    problems, _ = T.check(STORIES, feats)
    assert len(problems) == 3 and "R090" in problems[0] and "JS07" in problems[1] and "has no @R tag" in problems[2]


def test_a_malformed_tag_fails():
    feats = [_feat(["JS01"], ("a", ["R010"]), ("b", ["R020", "R06"])), _feat(["JS02", "slow"], ("c", ["R010", "JS01"]))]
    problems, _ = T.check(STORIES, feats)
    assert len(problems) == 3 and "R06" in problems[0] and "slow" in problems[1] and "JS01" in problems[2]


def test_eval_on_the_stub_and_the_gate(tmp_path, monkeypatch):
    log = eval_held_file.run(log_dir=tmp_path / "good")
    assert {s.id: s.scores["cause_in_code"].answer for s in log.samples} == {
        "half-file": "partial_file", "category-missing": "category_missing"}
    assert gate.passes(tmp_path / "good", 1.0) == (True, 1.0)
    # a model that names the same cause for every file is right on one case of two: the gate fails
    always = lambda mode="stub", **kw: __import__("models").StubAgentModel(   # noqa: E731
        [("export_log", {})], lambda results: '{"cause": "partial_file", "evidence": ["guess"]}')
    monkeypatch.setattr(eval_held_file.half_file_agent, "stub_model", always)
    eval_held_file.run(log_dir=tmp_path / "same")
    assert gate.passes(tmp_path / "same", 1.0) == (False, 0.5)
