"""Every job story has a feature, every requirement and constraint has a scenario, and every tag names something.

Reads docs/requirements.md (a heading `## JS01 ...` per job story; under it one line `- **R010** ...` per
requirement, or a table row `| R010 | ...`) and the behave features (a feature tagged @JS01, its scenarios tagged
@R010). A requirement code is used once in the whole file, also across job stories: R010, R020 ... by tens, or a
team's own R10, R20 ... with sub-requirements R11, R21. A code defined twice fails. A scenario's @R tag must name a
requirement listed under its feature's story.

Constraints and requests (the RF and RQ codes of a feedback issue) are listed in their own section of
docs/requirements.md, `## Constraints and requests`, one table row `| RF010 | ...` or `| RQ010 | ...` each, each
kind with its own numbers. They hold for the whole system, so their tags are read across all features. An RF is a
property of the running system that a test can pass or fail: it needs at least one scenario tagged @RF010. An RQ is
written and read, not run: it needs no scenario and is listed. A scenario needs an @R or an @RF tag; an @RQ tag may
stand beside them. A tag that is not in the section fails.

    uv run python check_traceability.py          # exit 1 and one line per problem; else a count per story
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent
DOC = "docs/requirements.md"
OTHER_TAGS = {"eval", "limits", "live"}   # the only tags allowed beside @JSnn, @Rnn0, @RFnn0 and @RQnn0
SECTION = "## Constraints and requests"
REQ = r"R0\d{2}|R[1-9]\d+"                # R010, R020 ... or the teams' R10, R11, R20 ...; not R06
CODE = rf"JS\d\d|{REQ}|R[FQ]\d{{3}}"


def stories(text: str) -> dict[str, list[str]]:
    out, current = {}, None
    for line in text.splitlines():
        if m := re.match(r"## (JS\d\d) ", line):
            current = m.group(1)
            out[current] = []
        elif line.startswith("## "):
            current = None
        elif current and (m := re.match(rf"(?:- \*\*|\| )({REQ})(?:\*\*| \|)", line)):
            out[current].append(m.group(1))
    return out


def reused(reqs: dict[str, list[str]]) -> list[str]:
    """One problem for each requirement code defined more than once in the file."""
    first, problems = {}, []
    for story, rs in reqs.items():
        for r in rs:
            if r in first:
                problems.append(f"{story} {r}: the code is already defined under {first[r]}; a code is used once in the file")
            else:
                first[r] = story
    return problems


def constraints(text: str) -> dict[str, list[str]]:
    """The RF and RQ codes of the section `## Constraints and requests`, in their order."""
    out, inside = {"RF": [], "RQ": []}, False
    for line in text.splitlines():
        if line.startswith("## "):
            inside = line.strip() == SECTION
        elif inside and (m := re.match(r"\| (R[FQ])(\d{3}) \|", line)):
            out[m.group(1)].append(m.group(1) + m.group(2))
    return out


def check_constraints(stories_text: str, feats: list[dict]) -> tuple[list[str], dict[str, int], dict[str, int]]:
    """Problems, scenarios per RF, scenarios per RQ. Tag spelling is checked in `check`."""
    listed = constraints(stories_text)
    rf, rq = {c: 0 for c in listed["RF"]}, {c: 0 for c in listed["RQ"]}
    problems = []
    for f in feats:
        for sc in f["scenarios"]:
            for t in dict.fromkeys(sc["tags"] + f["tags"]):
                if re.fullmatch(r"R[FQ]\d{3}", t):
                    known = rf if t.startswith("RF") else rq
                    if t in known:
                        known[t] += sc["runs"]
                    else:
                        problems.append(f"{f['file']}: @{t} on '{sc['name']}' names no constraint or request in {DOC}")
    problems += [f"{c}: no scenario is tagged @{c}" for c, n in rf.items() if n == 0]
    return problems, rf, rq


def features(folder: Path) -> list[dict]:
    """Per feature file: its tags and, per scenario, the tags and how many times it runs (rows of Examples)."""
    out = []
    for path in sorted(folder.glob("*.feature")):
        tags, feature, pending = [], {"file": path.name, "tags": [], "scenarios": []}, []
        in_examples = False
        for line in path.read_text().splitlines():
            s = line.strip()
            if s.startswith("@"):
                pending += s.split()
            elif s.startswith("Feature:"):
                feature["tags"], pending = [t[1:] for t in pending], []
            elif s.startswith(("Scenario:", "Scenario Outline:")):
                feature["scenarios"].append({"name": s.split(":", 1)[1].strip(), "tags": [t[1:] for t in pending],
                                             "runs": 0 if s.startswith("Scenario Outline") else 1})
                pending, in_examples = [], False
            elif s.startswith("Examples:"):
                in_examples, header = True, True
            elif in_examples and s.startswith("|"):
                if header:
                    header = False
                else:
                    feature["scenarios"][-1]["runs"] += 1
        out.append(feature)
    return out


def check(stories_text: str, feats: list[dict]) -> tuple[list[str], dict]:
    reqs = stories(stories_text)
    problems, covered = reused(reqs), {s: {r: 0 for r in rs} for s, rs in reqs.items()}
    with_feature = set()
    for f in feats:
        js = [t for t in f["tags"] if re.fullmatch(r"JS\d\d", t)]
        if len(js) != 1:
            problems.append(f"{f['file']}: the feature needs exactly one @JS tag, found {js}")
            continue
        story = js[0]
        if story not in reqs:
            problems.append(f"{f['file']}: @{story} names no job story in {DOC}")
            continue
        with_feature.add(story)
        for name, tags in [(f["file"], f["tags"])] + [(sc["name"], sc["tags"]) for sc in f["scenarios"]]:
            odd = [t for t in tags if not re.fullmatch(CODE, t) and t not in OTHER_TAGS]
            odd += [t for t in tags if re.fullmatch(r"JS\d\d", t) and name != f["file"]]
            if odd:
                problems.append(f"{f['file']}: '{name}' has tags that name nothing: {odd}")
        for sc in f["scenarios"]:
            rs = [t for t in sc["tags"] + f["tags"] if re.fullmatch(REQ, t)]
            if not rs and not any(re.fullmatch(r"RF\d{3}", t) for t in sc["tags"] + f["tags"]):
                problems.append(f"{f['file']}: scenario '{sc['name']}' has no @R tag and no @RF tag")
            for r in rs:
                if r not in reqs[story]:
                    problems.append(f"{f['file']}: @{r} on '{sc['name']}' names no requirement of {story}")
                else:
                    covered[story][r] += sc["runs"]
    for story, rs in reqs.items():
        if story not in with_feature:
            problems.append(f"{story}: no feature is tagged @{story}")
        problems += [f"{story} {r}: no scenario is tagged @{r}" for r in rs if covered[story][r] == 0 and story in with_feature]
    problems += check_constraints(stories_text, feats)[0]
    return problems, covered


if __name__ == "__main__":
    problems, covered = check((ROOT / DOC).read_text(), features(ROOT / "features"))
    for p in problems:
        print("PROBLEM", p)
    for story, rs in covered.items():
        print(f"{story}: {len(rs)} requirements, {sum(rs.values())} scenarios  " + " ".join(f"{r}:{n}" for r, n in rs.items()))
    _, rf, rq = check_constraints((ROOT / DOC).read_text(), features(ROOT / "features"))
    if rf:
        print(f"constraints: {len(rf)} RF, {sum(rf.values())} scenarios  " + " ".join(f"{c}:{n}" for c, n in rf.items()))
    if rq:
        print(f"requests: {len(rq)} RQ, read, not run  " + " ".join(rq))
    sys.exit(1 if problems else 0)
