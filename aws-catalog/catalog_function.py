"""A catalog change on AWS: an EventBridge event from Glue starts this function, which drafts a description for
each new column and leaves it for a person.

    Glue UpdateTable  ->  EventBridge event "Glue Data Catalog Table State Change"  ->  this function
        ->  catalog_watch.compare (code): what changed, by VersionId
        ->  column_drafter.draft: one model call per new column, a check in code, at most 2 retries
        ->  pending/<table>.<column>.json in S3, status "draft". Nothing is written to the Glue Data Catalog.

The code is catalog_watch.py and column_drafter.py, unchanged. Glue sends these events best effort: "you cannot
write a program that depends on the order or existence of notification events, as they might be out of sequence
or missing" (Glue developer guide, automating-awsglue-with-cloudwatch-events). So the same function also runs a
daily check, from a schedule rule or a direct call with {"daily": true}: code compares every table with its stored
definition and drafts for any new column that no event announced.

Environment: BUCKET, DATABASE; COURSE_MCP_TOKEN (set at deploy, never logged); L19_DRAFT_MODEL=stub for the stub.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import boto3

import column_drafter
from glue_catalog import GlueCatalog
from models import DEFAULT_MODEL, gateway_chat


def build_chat():
    kind = os.environ.get("L19_DRAFT_MODEL", "gateway")
    if kind.startswith("stub"):
        return column_drafter.stub_chat(kind)
    return gateway_chat(os.environ.get("L19_MODEL") or DEFAULT_MODEL)


def handler(event, context, glue=None, s3=None, chat=None):
    if event.get("ping"):                    # `catalog-up` calls this once: every import worked, no model call
        return {"ok": True, "token_set": bool(os.environ.get("COURSE_MCP_TOKEN"))}
    started = time.monotonic()
    s3, bucket = s3 or boto3.client("s3"), os.environ["BUCKET"]
    column_drafter.CATALOG = GlueCatalog(glue or boto3.client("glue"), s3, bucket, os.environ["DATABASE"])
    calls: list[str] = []
    model = chat or build_chat()

    def counted(system: str, user: str) -> str:
        calls.append(user[:40])
        return model(system, user)

    column_drafter.CHAT = counted
    column_drafter.PENDING = Path("/tmp/pending")
    daily = bool(event.get("daily")) or event.get("detail-type") == "Scheduled Event"
    drafts = column_drafter.daily_handler(event) if daily else column_drafter.handler(event)
    out = []
    for d in drafts:
        local = Path(d.pop("pending_file"))
        key = f"pending/{d['table']}.{d['column']}.json"
        body = {**d, "found_by": "daily check" if daily else "event", "model_calls": len(calls),
                "event_time": event.get("time"), "event_id": event.get("id")}
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(body, indent=1).encode(), ContentType="application/json")
        local.unlink(missing_ok=True)
        out.append({**body, "pending_key": key})
    print(f"L19 catalog {'daily check' if daily else 'event ' + str(event.get('detail', {}).get('typeOfChange'))}: "
          f"{len(out)} drafts, {len(calls)} model calls, {round(time.monotonic() - started, 1)} s")
    return {"found_by": "daily check" if daily else "event", "drafts": out, "model_calls": len(calls)}
