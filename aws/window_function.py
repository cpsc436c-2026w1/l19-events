"""The Lambda function: stream_window.handler with its numbers written to a DynamoDB table.

The function's code is stream_window.py, unchanged. This file gives it a sink that writes to the window table
(local runs keep the numbers in memory) and reads one setting:

    L19_VALIDATE   "1": check every record against the input model, reject what does not fit (inputs.py)
                   "0": do not check; a record without a price raises and the invocation fails. `up` sets "0",
                        because the failing-batch run shows what Lambda does with a function that fails.

Rows in the window table (partition key pk, sort key sk):
    window#<store>      <window start>#<shard>   count, avg_price, burst   written when the window ends
    correction#<store>  <eventID>        window_start, price          a late update, for an earlier window
    rejected#<store>    <eventID>        currency, reason

Lambda keeps the window's state per shard and makes the last call of a window once per shard (services-ddb-windows:
"Your state can be a maximum of 1 MB for each shard"). A new on-demand table had 4 shards in our run, so one window
gave 4 last calls, each with its own part of the count. The row is therefore keyed by window and shard, and a
reader adds the rows of a window. (The first version wrote one row per window: the 4 calls overwrote each other
and the table showed 2 of 20 updates. output/aws-live/run-batch-second.txt.)
"""
from __future__ import annotations

import os
import statistics
from decimal import Decimal

import boto3

import stream_window

TABLE = boto3.resource("dynamodb").Table(os.environ["WINDOW_TABLE"])
VALIDATE = os.environ.get("L19_VALIDATE", "0") == "1"


class _Windows:
    shard = "local"                      # set from the event before each call

    def append(self, w) -> None:
        TABLE.put_item(Item={"pk": f"window#{w.store}", "sk": f"{w.window_start}#{self.shard}",
                             "window_start": w.window_start, "window_end": w.window_end,
                             "count": w.count, "avg_price": Decimal(str(w.avg_price)), "burst": w.burst})


class _Keyed:
    """Written by eventID: a retried invocation writes the same row again, not a second one."""

    def __init__(self, kind: str):
        self.kind = kind

    def __setitem__(self, event_id: str, row) -> None:
        item = {k: (Decimal(str(v)) if isinstance(v, float) else v) for k, v in row.model_dump().items()}
        TABLE.put_item(Item={"pk": f"{self.kind}#{row.store}", "sk": event_id, **item})


class DynamoSink:
    def __init__(self):
        self.windows, self.corrections, self.rejected = _Windows(), _Keyed("correction"), _Keyed("rejected")

    def median_count(self, store: str) -> float | None:
        rows = TABLE.query(KeyConditionExpression="pk = :p", ExpressionAttributeValues={":p": f"window#{store}"},
                           ProjectionExpression="#c", ExpressionAttributeNames={"#c": "count"})["Items"]
        counts = [int(r["count"]) for r in rows]
        return statistics.median(counts) if len(counts) >= stream_window.MIN_HISTORY else None


stream_window.SINK = DynamoSink()


def _counted(state: dict) -> int:
    return sum(s["count"] for s in (state or {}).get("stores", {}).values())


def handler(event, context):
    """One log line per invocation (CloudWatch Logs): what came in and what state went back, or the error."""
    line = (f"shard ..{event.get('shardId', 'local')[-4:]} window {event['window']['start'][11:19]} records {len(event['Records'])} "
            f"final {event.get('isFinalInvokeForWindow')} early {event.get('isWindowTerminatedEarly')} "
            f"state_in {_counted(event.get('state'))}")
    stream_window.SINK.windows.shard = event.get("shardId", "local")
    try:
        out = stream_window.handler(event, context, validate=VALIDATE)
    except Exception as e:
        print(f"L19 {line} -> raised {type(e).__name__}")
        raise
    print(f"L19 {line} -> state_out {_counted(out['state'])}")
    return out
