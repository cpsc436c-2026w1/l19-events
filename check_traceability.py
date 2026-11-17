"""Every job story has a feature, every requirement has a scenario, and every tag names something.

Reads STORIES.md (a heading `## JS01 ...` per job story, a table row `| R010 | ...` per requirement) and the
behave features (a feature tagged @JS01, its scenarios tagged @R010). Requirement numbers start again in each
story, so a scenario's @R tag is read inside its feature's story.

    uv run python check_traceability.py          # exit 1 and one line per problem; else a count per story
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent


def stories(text: str) -> dict[str, list[str]]:
    out, current = {}, None
    for line in text.splitlines():
        if m := re.match(r"## (JS\d\d) ", line):
            current = m.group(1)
            out[current] = []
        elif current and (m := re.match(r"\| (R\d{3}) \|", line)):
            out[current].append(m.group(1))
    return out


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
    problems, covered = [], {s: {r: 0 for r in rs} for s, rs in reqs.items()}
    with_feature = set()
    for f in feats:
        js = [t for t in f["tags"] if re.fullmatch(r"JS\d\d", t)]
        if len(js) != 1:
            problems.append(f"{f['file']}: the feature needs exactly one @JS tag, found {js}")
            continue
        story = js[0]
        if story not in reqs:
            problems.append(f"{f['file']}: @{story} names no job story in STORIES.md")
            continue
        with_feature.add(story)
        for sc in f["scenarios"]:
            rs = [t for t in sc["tags"] + f["tags"] if re.fullmatch(r"R\d{3}", t)]
            if not rs:
                problems.append(f"{f['file']}: scenario '{sc['name']}' has no @R tag")
            for r in rs:
                if r not in reqs[story]:
                    problems.append(f"{f['file']}: @{r} on '{sc['name']}' names no requirement of {story}")
                else:
                    covered[story][r] += sc["runs"]
    for story, rs in reqs.items():
        if story not in with_feature:
            problems.append(f"{story}: no feature is tagged @{story}")
        problems += [f"{story} {r}: no scenario is tagged @{r}" for r in rs if covered[story][r] == 0 and story in with_feature]
    return problems, covered


if __name__ == "__main__":
    problems, covered = check((ROOT / "STORIES.md").read_text(), features(ROOT / "features"))
    for p in problems:
        print("PROBLEM", p)
    for story, rs in covered.items():
        print(f"{story}: {len(rs)} requirements, {sum(rs.values())} scenarios  " + " ".join(f"{r}:{n}" for r, n in rs.items()))
    sys.exit(1 if problems else 0)
