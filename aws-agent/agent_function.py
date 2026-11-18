"""The agent for a held export file as a Lambda function, started by S3 when a new file lands in prices_raw/.

    S3 event  ->  runbook in code (batch_check.check)  ->  only for an unsettled file: the Strands agent
              ->  code checks the cause it names (half_file_agent.verify)  ->  results/day=<day>/<store>.json

The code is batch_check.py and half_file_agent.py, unchanged; this file reads the event, builds the model and the
S3 lake, and writes the result for a person. Nothing here changes a file, the job or the catalog.

Strands in Lambda (strandsagents.com, user guide, "Deploy to AWS Lambda", read 2026-11-17): the handler is a
normal `handler(event, context)`; dependencies are installed for the function's platform (`--platform
manylinux2014_aarch64 --only-binary=:all:`); the Lambda example does not stream its response. The official Strands
layer carries the base strands-agents package at 1.40.0 (layer version 2), so this function ships its own dependencies
(strands-agents[openai] 1.64.0, the version the hooks were written for).

S3 invokes the function asynchronously (S3 user guide, "Event notification types and destinations"; Lambda
developer guide, "Process Amazon S3 event notifications with Lambda"). The trigger is limited to the prefix
prices_raw/ and the suffix .csv, and the function writes only under results/, so its own output cannot start it
again. `agent-up` sets the asynchronous retries to 0: a failed run is not repeated, so the model calls stay bounded.

Limits, in code: 12 model calls and 10 tool calls in a Strands hook (half_file_agent.CallCap), and a deadline of
240 s. The deadline is checked before each model call and one call may take 300 s, so the function's timeout is
600 s: the function ends the agent and still writes its result.

Environment: BUCKET; COURSE_MCP_TOKEN (the course token, set at deploy and never logged); GATEWAY_URL and
L19_MODEL (optional); L19_AGENT_MODEL=stub runs the stub model, without a network call; L19_MAX_MODEL_CALLS
lowers the cap on model calls for one file (set at `agent-up` from your environment).
"""
from __future__ import annotations

import json
import os
import time

import boto3

import half_file_agent
from local import object_key
from models import DEFAULT_MODEL, gateway_model
from s3_lake import S3Lake

DEADLINE_S = 240


def build_model():
    kind = os.environ.get("L19_AGENT_MODEL", "gateway")
    if kind.startswith("stub"):
        return half_file_agent.stub_model(kind)
    return gateway_model(os.environ.get("L19_MODEL") or DEFAULT_MODEL)


def handler(event, context, s3=None, model=None):
    if event.get("ping"):               # `agent-up` calls this once: every import worked, and no model was called
        import strands
        return {"ok": True, "strands": getattr(strands, "__version__", "installed"),
                "token_set": bool(os.environ.get("COURSE_MCP_TOKEN"))}
    s3, bucket, out = s3 or boto3.client("s3"), os.environ["BUCKET"], []
    for record in event["Records"]:
        started = time.monotonic()
        key = object_key(record)
        lake = S3Lake(s3, bucket)
        lake.get_object(key)
        arrived = lake.arrived_at(key, record["eventTime"][:19] + "Z")
        m, trace = model or build_model(), []
        result = half_file_agent.triage(key, arrived, lake, m, caps=(
            int(os.environ.get("L19_MAX_MODEL_CALLS") or half_file_agent.MAX_MODEL_CALLS),
            half_file_agent.MAX_TOOL_CALLS, DEADLINE_S), trace=trace)
        day = key.split("day=")[1].split("/")[0]
        body = {**result.model_dump(), "arrived_at": arrived, "seconds": round(time.monotonic() - started, 1),
                "tools_called": [t["tool"] for t in trace if "tool" in t],
                "model_calls_log": next((t["model_calls"] for t in trace if "model_calls" in t), []),
                "request_id": getattr(context, "aws_request_id", ""),
                "event_key": record["s3"]["object"]["key"], "event_version": record.get("eventVersion")}
        s3.put_object(Bucket=bucket, Key=f"results/day={day}/{result.store}.json",
                      Body=json.dumps(body, indent=1).encode(), ContentType="application/json")
        print(f"L19 agent {result.store}: code {result.code_verdict}, decided by {result.decided_by}, cause "
              f"{result.cause}, verified {result.verified}, fix {result.fix}, {result.model_calls} model calls, "
              f"{result.tool_calls} tool calls, {body['seconds']} s")
        out.append(body)
    return out
