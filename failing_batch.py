"""One record of a batch makes the function fail: what Lambda does with the batch, simulated locally.

The planted batch (events/price-updates/failing-batch.json) has 8 records for one window; the sixth lacks a price.
The function here does not check its input (validate=False), so it raises on that record and the whole
invocation fails. With these settings on the event source mapping:

    BisectBatchOnFunctionError = true     a failed batch is split into two smaller batches and each is retried;
                                          splitting does not use up the retry quota
    MaximumRetryAttempts = 0              a batch that cannot be split further is not retried again
    DestinationConfig.OnFailure           a discarded batch is described to a queue, topic or bucket: metadata about
                                          the failed invocation (the sequence numbers), not the record itself

Lambda developer guide: services-dynamodb-errors ("Retain discarded records for a DynamoDB event source"),
services-ddb-batchfailurereporting ("Bisecting a batch"); API reference: UpdateEventSourceMapping. With the
default settings a bad record can block its shard for up to one day (services-dynamodb-errors).
Assumed, not read in the documentation: the order in which the two halves are retried (here: in stream order).

The state of a tumbling window comes back only from an invocation that succeeds, so a failed invocation leaves
no count behind; the records before the bad one are applied when their half succeeds, once. A later delivery of
the same batch does not change a count: every eventID is already in the state.

    uv run python failing_batch.py
"""
from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel

import stream_window

EVENT = Path(__file__).parent / "events" / "price-updates" / "failing-batch.json"


class BisectReport(BaseModel):
    invocations: list[str]                 # batch size and outcome of each invocation, in order
    applied: int                           # records counted in the window's state
    failures: list[dict]                   # what the on-failure destination received
    counted_twice: int

    def line(self) -> str:
        f = self.failures[0] if self.failures else {}
        return (f"batch of 8: invocations {' '.join(self.invocations)}; {self.applied} records applied once, "
                f"{len(self.failures)} sent aside (sequence number ...{str(f.get('startSequenceNumber'))[-4:]}); "
                f"delivered again: {self.counted_twice} counted twice")


def deliver(records: list[dict], event: dict, state: dict, report: BisectReport, options: dict) -> dict:
    """One batch as the event source mapping handles it: invoke; on an error split in two and retry each half; a
    single record that still fails is discarded and described to the on-failure destination."""
    try:
        new = stream_window.handler({**event, "Records": records, "state": state}, **options)["state"]
        report.invocations.append(f"{len(records)}:ok")
        return new
    except Exception as e:                                   # the function raised: the invocation failed
        report.invocations.append(f"{len(records)}:{type(e).__name__}")
        if len(records) == 1:
            seq = records[0]["dynamodb"]["SequenceNumber"]
            report.failures.append({"startSequenceNumber": seq, "endSequenceNumber": seq, "batchSize": 1,
                                    "shardId": event["shardId"], "streamArn": event["eventSourceARN"],
                                    "condition": "RetryAttemptsExhausted"})
            return state
        half = len(records) // 2
        state = deliver(records[:half], event, state, report, options)
        return deliver(records[half:], event, state, report, options)


def run(validate: bool = False) -> BisectReport:
    stream_window.SINK = stream_window.Sink()
    event = json.loads(EVENT.read_text())
    report = BisectReport(invocations=[], applied=0, failures=[], counted_twice=0)
    options = {"validate": validate}
    state = deliver(event["Records"], event, {}, report, options)
    report.applied = sum(s["count"] for s in state["stores"].values())
    good = [r for r in event["Records"] if "price" in r["dynamodb"]["NewImage"]]
    again = stream_window.handler({**event, "Records": good, "state": state}, **options)["state"]   # the batch again
    report.counted_twice = sum(s["count"] for s in again["stores"].values()) - report.applied
    return report


if __name__ == "__main__":
    print(run().line())
    checked = run(validate=True)
    print(f"with the input check: invocations {' '.join(checked.invocations)}; {checked.applied} applied, "
          f"{len(stream_window.SINK.rejected)} rejected at the boundary "
          f"({next(iter(stream_window.SINK.rejected.values())).reason}), 0 sent aside")
