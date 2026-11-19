"""The traceability check, the eval on the stub and the gate."""
from pathlib import Path

import check_traceability as T
import eval_held_file
import gate

ROOT = Path(__file__).parent.parent
# codes are used once in the file, also across stories; a requirement is a line `- **R010** ...` or a table row
STORIES = "## JS01 A story\n- **R010** x\n- **R020** y\n## JS02 Another\n| R030 | z |\n"


def _feat(tags, *scenarios):
    return {"file": "f.feature", "tags": tags, "scenarios": [{"name": n, "tags": t, "runs": 1} for n, t in scenarios]}


def test_the_repository_is_traceable():
    problems, covered = T.check((ROOT / "docs/requirements.md").read_text(), T.features(ROOT / "features"))
    assert problems == []
    assert {s: sum(r.values()) for s, r in covered.items()} == {"JS01": 14, "JS02": 19, "JS03": 11, "JS04": 7}


def test_a_story_without_a_feature_fails():
    problems, _ = T.check(STORIES, [_feat(["JS01"], ("a", ["R010"]), ("b", ["R020"]))])
    assert problems == ["JS02: no feature is tagged @JS02"]


def test_a_requirement_without_a_scenario_fails():
    problems, _ = T.check(STORIES, [_feat(["JS01"], ("a", ["R010"])), _feat(["JS02"], ("c", ["R030"]))])
    assert problems == ["JS01 R020: no scenario is tagged @R020"]


def test_a_tag_that_names_nothing_fails():
    feats = [_feat(["JS01"], ("a", ["R010"]), ("b", ["R020"]), ("d", ["R090"])), _feat(["JS02"], ("c", ["R030"])),
             _feat(["JS07"], ("e", ["R030"])), _feat(["JS02"], ("untagged", []))]
    problems, _ = T.check(STORIES, feats)
    assert len(problems) == 3 and "R090" in problems[0] and "JS07" in problems[1] and "has no @R tag" in problems[2]


def test_a_code_defined_under_two_stories_fails():
    doc = "## JS01 A story\n- **R010** x\n## JS02 Another\n- **R010** z\n"
    problems, _ = T.check(doc, [_feat(["JS01"], ("a", ["R010"])), _feat(["JS02"], ("c", ["R010"]))])
    assert problems == ["JS02 R010: the code is already defined under JS01; a code is used once in the file"]


def test_a_tag_of_another_story_fails():
    problems, _ = T.check(STORIES, [_feat(["JS01"], ("a", ["R010"]), ("b", ["R020"])), _feat(["JS02"], ("c", ["R030", "R010"]))])
    assert problems == ["f.feature: @R010 on 'c' names no requirement of JS02"]


def test_a_malformed_tag_fails():
    feats = [_feat(["JS01"], ("a", ["R010"]), ("b", ["R020", "R06"])), _feat(["JS02", "slow"], ("c", ["R030", "JS01"]))]
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


# ---- constraints (RF) and requests (RQ) from the feedback --------------------------------------------------

CONSTRAINTS = ("## Constraints and requests\n| Code | Text |\n|---|---|\n"
               "| RF010 | counted once |\n| RF020 | late counts |\n| RQ010 | write it down |\n")


def test_constraints_and_requests_are_read_from_their_section():
    assert T.constraints(STORIES + CONSTRAINTS) == {"RF": ["RF010", "RF020"], "RQ": ["RQ010"]}
    assert T.constraints(STORIES) == {"RF": [], "RQ": []}
    assert T.stories(STORIES + CONSTRAINTS) == T.stories(STORIES)     # the section's rows are not requirements


def test_every_constraint_has_a_scenario_and_a_request_needs_none():
    feats = [_feat(["JS01"], ("a", ["R010", "RF010"]), ("b", ["R020"]), ("c", ["RF020"]), ("d", ["RF020"])),
             _feat(["JS02"], ("e", ["R030"]))]
    problems, covered = T.check(STORIES + CONSTRAINTS, feats)
    assert problems == []
    assert T.check_constraints(STORIES + CONSTRAINTS, feats)[1:] == ({"RF010": 1, "RF020": 2}, {"RQ010": 0})


def test_a_constraint_without_a_scenario_fails():
    feats = [_feat(["JS01"], ("a", ["R010", "RF010"]), ("b", ["R020"])), _feat(["JS02"], ("e", ["R030"]))]
    problems, _ = T.check(STORIES + CONSTRAINTS, feats)
    assert problems == ["RF020: no scenario is tagged @RF020"]


def test_a_scenario_may_carry_an_rf_tag_instead_of_an_r_tag_and_an_rq_tag_beside_it():
    feats = [_feat(["JS01"], ("a", ["R010"]), ("b", ["R020"]), ("c", ["RF010", "RQ010"]), ("d", ["RF020"])),
             _feat(["JS02"], ("e", ["R030"]))]
    assert T.check(STORIES + CONSTRAINTS, feats)[0] == []


def test_an_rq_tag_alone_does_not_make_a_scenario_traceable():
    feats = [_feat(["JS01"], ("a", ["R010", "RF010", "RF020"]), ("b", ["R020"]), ("c", ["RQ010"])),
             _feat(["JS02"], ("e", ["R030"]))]
    problems, _ = T.check(STORIES + CONSTRAINTS, feats)
    assert problems == ["f.feature: scenario 'c' has no @R tag and no @RF tag"]


def test_an_unknown_or_malformed_constraint_tag_fails():
    feats = [_feat(["JS01"], ("a", ["R010", "RF010", "RF020"]), ("b", ["R020", "RF090"]), ("c", ["R010", "RQ020"]),
                   ("d", ["R010", "RF01"])), _feat(["JS02"], ("e", ["R030"]))]
    problems, _ = T.check(STORIES + CONSTRAINTS, feats)
    assert len(problems) == 3
    assert "RF01" in problems[0] and "RF090" in problems[1] and "RQ020" in problems[2]
    assert "names no constraint or request" in problems[1]


def test_without_the_section_an_rf_tag_names_nothing():
    feats = [_feat(["JS01"], ("a", ["R010", "RF010"]), ("b", ["R020"])), _feat(["JS02"], ("e", ["R030"]))]
    problems, _ = T.check(STORIES, feats)
    assert problems == ["f.feature: @RF010 on 'a' names no constraint or request in docs/requirements.md"]



def test_the_repository_lists_its_constraints_and_requests():
    rf_problems, rf, rq = T.check_constraints((ROOT / "docs/requirements.md").read_text(), T.features(ROOT / "features"))
    assert rf_problems == [] and rf == {"RF010": 1, "RF020": 1, "RF030": 1} and list(rq) == ["RQ010"]


# ---- the teams' own numbering: R10, R20 ... with sub-requirements R11, R21, beside R010 -------------------

TEAM = "## JS01 A story\n| R10 | x |\n| R11 | x, part |\n| R20 | y |\n| R010 | z |\n"


def test_the_teams_numbering_is_read():
    assert T.stories(TEAM) == {"JS01": ["R10", "R11", "R20", "R010"]}


def test_r10_r11_and_r010_tags_trace_to_their_requirements():
    feats = [_feat(["JS01"], ("a", ["R10"]), ("b", ["R11"]), ("c", ["R20", "R010"]))]
    problems, covered = T.check(TEAM, feats)
    assert problems == [] and covered == {"JS01": {"R10": 1, "R11": 1, "R20": 1, "R010": 1}}


def test_a_team_code_without_a_scenario_or_unknown_fails():
    feats = [_feat(["JS01"], ("a", ["R10"]), ("b", ["R20", "R010", "R30"]))]
    problems, _ = T.check(TEAM, feats)
    assert problems == ["f.feature: @R30 on 'b' names no requirement of JS01", "JS01 R11: no scenario is tagged @R11"]
