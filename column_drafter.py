"""A new column without a description: one model call drafts it, code checks it, a person accepts it.

Evaluator-optimizer with a code check (recipes workflow-patterns, main/evaluator.py): one model call writes the
draft, `validate` in app code passes it or says what to change, and app code asks again with that feedback, at
most MAX_RETRIES times. Who decides the next step: app code. The result is written to pending/ and waits for a
person; nothing here writes to the Glue Data Catalog.

    uv run python column_drafter.py                           # stub model, the new-column case
    uv run python column_drafter.py --model gateway           # the course LLM gateway (RUN.md)
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Callable

import catalog_watch
from local import LocalCatalog
from models import gateway_chat, parse_json
from results import Draft

MAX_RETRIES = 2                       # so at most 3 model calls for one column
MAX_COMMENT = 255                     # the catalog's limit for a column comment (L18 storeplan/glue_desc.py)
PENDING = Path(__file__).parent / "pending"
ABSOLUTE = ("always", "never", "every", "all rows", "all products", "guarantee", "must be")

SYSTEM = f"""You write the description of one new column of a table in a data catalog. You are given the table,
the column's name and type, and sample values. Describe only what the name, the type and the samples show.
Reply with JSON only: {{"description": "...", "sample_values_used": ["..."]}}
The description is one sentence of at most {MAX_COMMENT} characters, names the column's type, and makes no claim
about values you were not shown. sample_values_used lists the sample values your description rests on."""


def validate(reply: dict, column_type: str, samples: list) -> list[str]:
    """The evaluator, in app code. An empty list means the draft passes."""
    shown = {json.dumps(s) if not isinstance(s, str) else s for s in samples}
    description, used = reply.get("description"), reply.get("sample_values_used")
    if not isinstance(description, str) or not description.strip():
        return ["the reply has no description; reply with the JSON object only"]
    problems = []
    if len(description) > MAX_COMMENT:
        problems.append(f"the description has {len(description)} characters; the limit is {MAX_COMMENT}")
    if column_type.lower() not in description.lower():
        problems.append(f"the description does not name the type {column_type}")
    if not isinstance(used, list) or not used:
        problems.append("sample_values_used is empty; list the sample values the description rests on")
    else:
        beyond = [u for u in used if (json.dumps(u) if not isinstance(u, str) else u) not in shown
                  and str(u).lower() not in {s.lower() for s in shown}]
        if beyond:
            problems.append(f"sample_values_used names values that were not in the samples: {beyond}")
    numbers = [n for n in re.findall(r"\d+(?:\.\d+)?", description) if n not in " ".join(shown)]
    if numbers:
        problems.append(f"the description gives numbers that are not in the samples: {numbers}")
    absolute = [w for w in ABSOLUTE if re.search(rf"\b{w}\b", description.lower())]
    if absolute:
        problems.append(f"the description claims more than {len(samples)} samples can show: remove {absolute}")
    return problems


def draft(table: str, column: str, column_type: str, samples: list, chat: Callable[[str, str], str],
          max_retries: int | None = None) -> Draft:
    task = (f"Table: {table}\nColumn: {column}\nType: {column_type}\n"
            f"Sample values ({len(samples)}): {json.dumps(samples)}")
    user, reply, problems = task, {}, ["no reply"]
    for attempt in range(1, (MAX_RETRIES if max_retries is None else max_retries) + 2):
        reply = parse_json(chat(SYSTEM, user))                       # the one model call of this round
        problems = validate(reply, column_type, samples)             # the evaluator: a check in code
        if not problems:
            break
        user = (f"{task}\nYour last reply: {json.dumps(reply)[:600]}\nIt was rejected: {'; '.join(problems)}\n"
                "Write it again.")
    used = [u if isinstance(u, str) else json.dumps(u) for u in (reply.get("sample_values_used") or [])
            if isinstance(u, (str, int, float, bool))]
    return Draft(table=table, column=column, type=column_type, status="cannot_describe" if problems else "draft",
                 description="" if problems else reply["description"].strip(), sample_values_used=used,
                 attempts=attempt, problems=problems)


def write_pending(d: Draft, folder: Path | None = None) -> Path:
    """The draft waits here for a person. Accepting it (and writing it to the catalog) is another step."""
    folder = folder or PENDING
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{d.table}.{d.column}.json"
    path.write_text(json.dumps(d.model_dump(), indent=1) + "\n")
    return path


CATALOG: LocalCatalog | None = None   # set by the caller
CHAT: Callable[[str, str], str] | None = None


def handler(event: dict, context=None) -> list[dict]:
    """Lambda entry point for the Glue catalog event: watch the table in code, then draft a description for each
    new column that lacks one."""
    catalog_watch.CATALOG = CATALOG
    return _draft_all(catalog_watch.handler(event))


def daily_handler(event: dict, context=None) -> list[dict]:
    """Lambda entry point for the daily check: Glue's events are best effort, so once a day code compares every
    table with its stored definition and drafts for any new column an event did not announce.
    event: {"max_retries": n} is optional."""
    catalog_watch.CATALOG = CATALOG
    return _draft_all(catalog_watch.daily_handler(event), event.get("max_retries"))


def _draft_all(results: list[dict], max_retries: int | None = None) -> list[dict]:
    out = []
    for result in results:
        if "draft_description" not in result["actions"]:
            continue
        cols = {c["Name"]: c for c in CATALOG.get_table("", result["table"])["Table"]["StorageDescriptor"]["Columns"]}
        for column in result["lacking_description"]:
            d = draft(result["table"], column, cols[column]["Type"], CATALOG.samples(result["table"], column), CHAT,
                      max_retries)
            out.append({**d.model_dump(), "pending_file": str(write_pending(d))})
    return out


def stub_chat(mode: str = "stub") -> Callable[[str, str], str]:
    """stub: a first draft that is too sure of itself, then one that passes. stub-stubborn: never passes."""
    good = {"description": "Boolean flag from the store's export; true and false both appear in the sample rows, "
                           "and the name suggests it marks a product on promotion.",
            "sample_values_used": ["true", "false"]}
    bad = {"description": "Every product on promotion is always marked true here.", "sample_values_used": ["yes"]}

    def chat(system: str, user: str) -> str:
        return json.dumps(good if mode == "stub" and "rejected" in user else bad)
    return chat


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--case", default="new-column")
    ap.add_argument("--model", default="stub", help="stub, stub-stubborn, gateway or gateway:<model>")
    a = ap.parse_args()
    CATALOG = LocalCatalog(a.case)
    CHAT = stub_chat(a.model) if a.model.startswith("stub") else gateway_chat(*a.model.split(":")[1:])
    for e in CATALOG.events():
        print(json.dumps(handler(e), indent=1))
