"""The three cases on AWS: create the resources, run a case, delete everything.

    uv run python -m aws.deploy up            # tables, queue, role, function, event source mapping (about 1 min)
    uv run python -m aws.deploy run-race      # two writers at once on one item
    uv run python -m aws.deploy run-batch     # a failing record in a batch; waits for the 5-minute window to end
    uv run python -m aws.deploy run-load      # 50 clients read while updates are written (--consistent)
    uv run python -m aws.deploy down          # delete everything `up` made, and check that none of it remains
    uv run python -m aws.deploy logs          # one line per invocation of the function, last 20 minutes
    uv run python -m aws.deploy update-code   # upload the function again after a change to its files
    uv run python -m aws.deploy agent-up | agent-run | agent-down    # the agent for a held file: aws/agent.py

What `up` creates, every one tagged project=l19-events:
    l19-prices                 DynamoDB table (product_code, store), on demand, with a change stream
    l19-windows                DynamoDB table for the function's numbers
    l19-failures               SQS queue: the on-failure destination
    recipe-l19-events-lambda   IAM role of the function (not created when LAMBDA_ROLE_ARN is set)
    l19-window                 Lambda function: stream_window.py with aws/window_function.py
    the event source mapping from the table's stream to the function

Event source mapping settings, from the Lambda developer guide (read 2026-11-17):
    TumblingWindowInSeconds=300        services-ddb-params: 0 to 900. services-ddb-windows: the function gets the
                                       window's state with each batch and a last call with isFinalInvokeForWindow
    BisectBatchOnFunctionError=True    services-dynamodb-errors: a failed batch is split in two; splitting does not
                                       use up the retry quota
    MaximumRetryAttempts=1             services-ddb-params: default -1 (retry until the record expires, 24 hours)
    DestinationConfig.OnFailure        services-ddb-params: a standard SQS queue or SNS topic; the record describes
                                       the batch (sequence numbers), "the actual records aren't included"
services-ddb-windows says tumbling windows "fully support the existing retry policies maxRetryAttempts and
maxRecordAge". It does not mention bisecting or the on-failure destination. What a run showed (output/aws-live/,
2026-11-17): a failed batch of 7 was split (3 and 4, then 2 and 2, then 1 and 1), the record without a price failed
twice alone and went to the queue with batchSize 1, and after a failed call the next call got the state of the last
successful call of that shard. The state and the window's last call are per shard: 4 shards, 4 last calls.

The role may do only what the function does (role_policy below). To use a role you already have, set
LAMBDA_ROLE_ARN to its ARN; then no role is created and `down` leaves it alone.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import platform
import subprocess
import sys
import time
import uuid
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from aws import clients

ROOT = Path(__file__).parent.parent
PREFIX = os.environ.get("L19_PREFIX", "l19")
TAG = {"project": "l19-events"}
PRICES, WINDOWS, QUEUE, FUNCTION = f"{PREFIX}-prices", f"{PREFIX}-windows", f"{PREFIX}-failures", f"{PREFIX}-window"
ROLE = "recipe-l19-events-lambda"
MAX_READ_UNITS, MAX_WRITE_UNITS = 1000, 200      # a ceiling on the price table's on-demand throughput: a client
#                                                  that loops without end is throttled there, not billed without limit
PYDANTIC = "pydantic==2.14.3"                    # the version in uv.lock; the function's zip carries it
SOURCES = ["stream_window.py", "inputs.py", "results.py"]


def region() -> str:
    return os.environ.get("AWS_REGION") or boto3.session.Session().region_name or "ca-central-1"


def client(service: str, **config):
    return boto3.client(service, region_name=region(), config=Config(**config) if config else None)


def architecture() -> str:
    """The function's architecture: this machine's, so the wheels in the zip are built on the same kind."""
    return os.environ.get("L19_ARCH") or ("arm64" if platform.machine().lower() in ("arm64", "aarch64") else "x86_64")


# ---- configuration (pure functions, tested offline) -----------------------------------------------------------

def mapping_config(stream_arn: str, queue_arn: str) -> dict:
    return {"FunctionName": FUNCTION, "EventSourceArn": stream_arn,
            "StartingPosition": "TRIM_HORIZON",         # from the oldest record the stream still holds
            "BatchSize": 10, "MaximumBatchingWindowInSeconds": 1,
            "TumblingWindowInSeconds": 300,
            "BisectBatchOnFunctionError": True,
            "MaximumRetryAttempts": 1,
            "DestinationConfig": {"OnFailure": {"Destination": queue_arn}}}


def role_policy(account: str, stream_arn: str) -> list[dict]:
    """What the function's role may do: these actions on these resources, nothing else."""
    r = region()
    return [
        {"Effect": "Allow", "Resource": stream_arn,                                    # read the change stream
         "Action": ["dynamodb:GetRecords", "dynamodb:GetShardIterator", "dynamodb:DescribeStream", "dynamodb:ListStreams"]},
        {"Effect": "Allow", "Resource": f"arn:aws:dynamodb:{r}:{account}:table/{WINDOWS}",   # write and read its numbers
         "Action": ["dynamodb:PutItem", "dynamodb:Query"]},
        {"Effect": "Allow", "Resource": f"arn:aws:sqs:{r}:{account}:{QUEUE}", "Action": ["sqs:SendMessage"]},
        {"Effect": "Allow", "Resource": f"arn:aws:logs:{r}:{account}:log-group:/aws/lambda/{FUNCTION}:*",
         "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]}]


def function_zip(vendor: Path | None = None) -> bytes:
    """stream_window.py and what it imports, the entry module, and pydantic's files from `vendor`."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name in SOURCES:
            z.write(ROOT / name, name)
        z.write(ROOT / "aws" / "window_function.py", "window_function.py")
        for path in sorted(vendor.rglob("*")) if vendor else []:
            if path.is_file() and "__pycache__" not in path.parts:
                z.write(path, path.relative_to(vendor).as_posix())
    return buf.getvalue()


def vendor_pydantic(arch: str) -> Path:
    """Download pydantic's wheels for the function's platform (Linux, Python 3.12) into build/lambda/."""
    target = ROOT / "build" / "lambda"
    plat = "aarch64-manylinux2014" if arch == "arm64" else "x86_64-manylinux2014"
    subprocess.run(["uv", "pip", "install", "--quiet", "--target", str(target), "--python-platform", plat,
                    "--python-version", "3.12", "--only-binary", ":all:", PYDANTIC], check=True)
    return target


# ---- what exists, and whose it is -----------------------------------------------------------------------------

def _missing(error: ClientError) -> bool:
    return error.response["Error"]["Code"] in ("ResourceNotFoundException", "NoSuchEntity", "QueueDoesNotExist",
                                               "AWS.SimpleQueueService.NonExistentQueue")


def existing(c: dict, account: str) -> dict[str, dict | None]:
    """For each name: None when it is not there, else its tags."""
    out: dict[str, dict | None] = {}
    for table in (PRICES, WINDOWS):
        try:
            arn = c["dynamodb"].describe_table(TableName=table)["Table"]["TableArn"]
            out[f"table {table}"] = {t["Key"]: t["Value"] for t in c["dynamodb"].list_tags_of_resource(ResourceArn=arn)["Tags"]}
        except ClientError as e:
            if not _missing(e):
                raise
            out[f"table {table}"] = None
    try:
        url = c["sqs"].get_queue_url(QueueName=QUEUE)["QueueUrl"]
        out[f"queue {QUEUE}"] = c["sqs"].list_queue_tags(QueueUrl=url).get("Tags", {})
    except ClientError as e:
        if not _missing(e):
            raise
        out[f"queue {QUEUE}"] = None
    try:
        out[f"function {FUNCTION}"] = c["lambda"].get_function(FunctionName=FUNCTION).get("Tags", {})
    except ClientError as e:
        if not _missing(e):
            raise
        out[f"function {FUNCTION}"] = None
    try:
        out[f"role {ROLE}"] = {t["Key"]: t["Value"] for t in c["iam"].list_role_tags(RoleName=ROLE)["Tags"]}
    except ClientError as e:
        if not _missing(e):
            raise
        out[f"role {ROLE}"] = None
    try:                                                             # a mapping can outlive its function
        ms = c["lambda"].list_event_source_mappings()["EventSourceMappings"]
        ours = [m["UUID"] for m in ms if m.get("FunctionArn", "").endswith(f":function:{FUNCTION}")
                and m.get("State") != "Deleting"]
        out[f"event source mapping of {FUNCTION}"] = {"project": TAG["project"], "uuids": ours} if ours else None
    except ClientError as e:
        if not _missing(e):
            raise
        out[f"event source mapping of {FUNCTION}"] = None
    try:
        groups = c["logs"].describe_log_groups(logGroupNamePrefix=f"/aws/lambda/{FUNCTION}")["logGroups"]
        out[f"log group /aws/lambda/{FUNCTION}"] = {} if any(
            g["logGroupName"] == f"/aws/lambda/{FUNCTION}" for g in groups) else None
    except ClientError as e:
        if not _missing(e):
            raise
        out[f"log group /aws/lambda/{FUNCTION}"] = None
    return out


def not_ours(found: dict) -> list[str]:
    """Names that exist without this project's tag (the log group is Lambda's and carries none)."""
    return [name for name, tags in found.items()
            if tags is not None and not name.startswith("log group") and tags.get("project") != TAG["project"]]


def clients_for() -> dict:
    return {s: client(s) for s in ("dynamodb", "sqs", "lambda", "iam", "logs", "sts")}


# ---- up -----------------------------------------------------------------------------------------------------

def up(c: dict | None = None, vendor: Path | None = None, sleep=time.sleep, smoke: bool = True) -> dict:
    c = c or clients_for()
    account = c["sts"].get_caller_identity()["Account"]
    foreign = not_ours(existing(c, account))
    if foreign:
        raise SystemExit(f"these names exist and are not tagged project=l19-events; nothing was changed: {foreign}")
    tags = [{"Key": k, "Value": v} for k, v in TAG.items()]
    ddb = c["dynamodb"]
    for table, extra in ((PRICES, {"StreamSpecification": {"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
                                    "OnDemandThroughput": {"MaxReadRequestUnits": MAX_READ_UNITS,
                                                           "MaxWriteRequestUnits": MAX_WRITE_UNITS},
                                    "keys": ("product_code", "store")}),
                         (WINDOWS, {"keys": ("pk", "sk")})):
        h, r = extra.pop("keys")
        try:
            ddb.create_table(TableName=table, BillingMode="PAY_PER_REQUEST", Tags=tags,
                             KeySchema=[{"AttributeName": h, "KeyType": "HASH"}, {"AttributeName": r, "KeyType": "RANGE"}],
                             AttributeDefinitions=[{"AttributeName": h, "AttributeType": "S"},
                                                   {"AttributeName": r, "AttributeType": "S"}], **extra)
        except ddb.exceptions.ResourceInUseException:
            pass                                                     # there from an earlier `up`: kept
    for table in (PRICES, WINDOWS):
        ddb.get_waiter("table_exists").wait(TableName=table, WaiterConfig={"Delay": 3, "MaxAttempts": 60})
    stream_arn = ddb.describe_table(TableName=PRICES)["Table"]["LatestStreamArn"]
    queue_url = c["sqs"].create_queue(QueueName=QUEUE, tags=TAG)["QueueUrl"]
    queue_arn = c["sqs"].get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    role_arn = os.environ.get("LAMBDA_ROLE_ARN", "")
    if not role_arn:
        trust = {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
        try:
            role_arn = c["iam"].create_role(RoleName=ROLE, AssumeRolePolicyDocument=json.dumps(trust), Tags=tags)["Role"]["Arn"]
            fresh = True
        except c["iam"].exceptions.EntityAlreadyExistsException:
            role_arn, fresh = c["iam"].get_role(RoleName=ROLE)["Role"]["Arn"], False
        c["iam"].put_role_policy(RoleName=ROLE, PolicyName="least-privilege", PolicyDocument=json.dumps(
            {"Version": "2012-10-17", "Statement": role_policy(account, stream_arn)}))
        if fresh:
            sleep(10)            # a new role takes a few seconds before Lambda can assume it (assumed: 10 s)

    lam, arch = c["lambda"], architecture()
    code = function_zip(vendor if vendor is not None else vendor_pydantic(arch))
    for attempt in range(6):
        try:
            lam.create_function(FunctionName=FUNCTION, Runtime="python3.12", Role=role_arn, Architectures=[arch],
                                Handler="window_function.handler", Code={"ZipFile": code}, Timeout=30, MemorySize=256,
                                Environment={"Variables": {"WINDOW_TABLE": WINDOWS, "L19_VALIDATE": "0"}}, Tags=TAG)
            break
        except lam.exceptions.ResourceConflictException:
            break                                                    # there from an earlier `up`: kept
        except lam.exceptions.InvalidParameterValueException as e:   # the new role is not assumable yet
            if "assume" not in str(e) or attempt == 5:
                raise
            sleep(5)
    lam.get_waiter("function_active_v2").wait(FunctionName=FUNCTION)
    out = {"account": account, "region": region(), "architecture": arch, "stream_arn": stream_arn,
           "queue_url": queue_url, "role_arn": role_arn, "zip_bytes": len(code)}
    if smoke:
        out["smoke"] = smoke_invoke(lam, ddb)
    if not lam.list_event_source_mappings(FunctionName=FUNCTION, EventSourceArn=stream_arn)["EventSourceMappings"]:
        lam.create_event_source_mapping(**mapping_config(stream_arn, queue_arn))
    for _ in range(40):
        (m,) = lam.list_event_source_mappings(FunctionName=FUNCTION, EventSourceArn=stream_arn)["EventSourceMappings"]
        if m["State"] == "Enabled":
            break
        sleep(3)
    out["mapping"] = {k: m.get(k) for k in ("State", "BatchSize", "TumblingWindowInSeconds", "BisectBatchOnFunctionError",
                                             "MaximumRetryAttempts", "StartingPosition")}
    return out


def smoke_invoke(lam, ddb) -> str:
    """Call the function directly with one record and then with the window's last call, before any stream record
    reaches it: an import error or a missing permission shows here, not as a "failing batch" later."""
    record = json.loads((ROOT / "events" / "price-updates" / "window-event.json").read_text())["Records"][0]
    record["dynamodb"]["NewImage"]["store"] = {"S": "smoke"}
    window = {"start": "2026-10-31T09:15:00Z", "end": "2026-10-31T09:20:00Z"}
    event = {"Records": [record], "window": window, "state": {}, "shardId": "smoke", "isFinalInvokeForWindow": False,
             "isWindowTerminatedEarly": False}
    state = {}
    for e in (event, {**event, "Records": [], "isFinalInvokeForWindow": True}):
        resp = lam.invoke(FunctionName=FUNCTION, Payload=json.dumps({**e, "state": state}).encode())
        body = json.loads(resp["Payload"].read())
        if resp.get("FunctionError"):
            raise SystemExit(f"the function fails on a good record: {str(body)[:400]}")
        state = body["state"]
    row = ddb.get_item(TableName=WINDOWS, Key={"pk": {"S": "window#smoke"}, "sk": {"S": window["start"] + "#smoke"}},
                       ConsistentRead=True).get("Item")
    if not row or row["count"]["N"] != "1":
        raise SystemExit(f"the function ran but did not store its window row: {row}")
    return "direct invoke: 1 record counted, the window row is in the table"


# ---- down ---------------------------------------------------------------------------------------------------

def down(c: dict | None = None, sleep=time.sleep) -> dict:
    """Delete what `up` made, by its exact names and only when it carries this project's tag; then look again."""
    c = c or clients_for()
    account = c["sts"].get_caller_identity()["Account"]
    found = existing(c, account)
    foreign = not_ours(found)
    if foreign:
        raise SystemExit(f"not tagged project=l19-events, so not deleted: {foreign}")
    lam, ddb = c["lambda"], c["dynamodb"]
    for uid in (found[f"event source mapping of {FUNCTION}"] or {}).get("uuids", []):
        lam.delete_event_source_mapping(UUID=uid)
    if found[f"function {FUNCTION}"] is not None:
        lam.delete_function(FunctionName=FUNCTION)
    if found[f"log group /aws/lambda/{FUNCTION}"] is not None:
        c["logs"].delete_log_group(logGroupName=f"/aws/lambda/{FUNCTION}")
    if found[f"queue {QUEUE}"] is not None:
        c["sqs"].delete_queue(QueueUrl=c["sqs"].get_queue_url(QueueName=QUEUE)["QueueUrl"])
    for table in (PRICES, WINDOWS):
        if found[f"table {table}"] is not None:
            ddb.delete_table(TableName=table)
    if found[f"role {ROLE}"] is not None and not os.environ.get("LAMBDA_ROLE_ARN"):
        for policy in c["iam"].list_role_policies(RoleName=ROLE)["PolicyNames"]:
            c["iam"].delete_role_policy(RoleName=ROLE, PolicyName=policy)
        c["iam"].delete_role(RoleName=ROLE)
    left: dict = {}
    for _ in range(40):                                              # tables and the queue take a moment to go
        left = {k: v for k, v in existing(c, account).items() if v is not None}
        if os.environ.get("LAMBDA_ROLE_ARN"):
            left.pop(f"role {ROLE}", None)
        if not left:
            break
        sleep(5)
    return {"deleted": [k for k, v in found.items() if v is not None], "remaining": sorted(left)}


def function_log(logs, since_s: int) -> list[str]:
    """The lines the function printed (aws/window_function.py), oldest first, with the time of each."""
    out, token = [], {}
    while True:
        page = logs.filter_log_events(logGroupName=f"/aws/lambda/{FUNCTION}", filterPattern='"L19 shard"',
                                      startTime=int((time.time() - since_s) * 1000), **token)
        out += [time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000)) + " " + e["message"].strip()[4:]
                for e in page["events"]]
        if not page.get("nextToken"):
            return out
        token = {"nextToken": page["nextToken"]}


# ---- the runs -------------------------------------------------------------------------------------------------

def mask(text: str, account: str) -> str:
    return text.replace(account, "123456789012")


def emit(name: str, lines: list[str], account: str, data: dict | None = None) -> None:
    """Print the lines and save them (and the numbers) under output/aws-live/, with the account id masked."""
    folder = ROOT / "output" / "aws-live"
    folder.mkdir(parents=True, exist_ok=True)
    text = mask("\n".join(lines) + "\n", account)
    print(text, end="")
    (folder / f"{name}.txt").write_text(text)
    if data is not None:
        (folder / f"{name}.json").write_text(mask(json.dumps(data, indent=1, default=str) + "\n", account))


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["up", "update-code", "logs", "run-race", "run-batch", "run-load", "down",
                                        "agent-up", "agent-run", "agent-down"])
    ap.add_argument("--consistent", action="store_true", help="run-load: strongly consistent reads")
    ap.add_argument("--users", type=int, default=50)
    ap.add_argument("--seconds", type=float, default=30.0)
    a = ap.parse_args(argv)
    c = clients_for()
    account = c["sts"].get_caller_identity()["Account"]
    run_id = uuid.uuid4().hex[:6]
    if a.command.startswith("agent-"):
        from aws import agent
        c["s3"] = client("s3")
        if a.command == "agent-up":
            token = next((os.environ[k].strip() for k in ("KB_MCP_TOKEN", "LLM_API_KEY", "COURSE_MCP_TOKEN")
                          if os.environ.get(k, "").strip()), "")
            out = agent.up(c, region(), architecture(), token)
            emit("agent-up", [f"bucket {out['bucket']}, function {agent.FUNCTION} ({out['architecture']}, zip "
                              f"{out['zip_bytes']} bytes, timeout {out['timeout_s']} s), model {out['model']}",
                              f"direct call: {out['ping']}",
                              "trigger: s3:ObjectCreated:* under prices_raw/ ending in .csv; asynchronous retries 0"],
                 account, out)
        elif a.command == "agent-run":
            started = time.time()
            r = agent.run(c, region())
            time.sleep(8)                                            # the REPORT lines reach the log a moment later
            emit("agent-run", agent.run_lines(r, agent.durations(c["logs"], started - 5)), account, r)
            sys.exit(0 if len(r["results"]) == len(agent.CASES) else 1)
        else:
            out = agent.down(c, region())
            emit("agent-down", [f"deleted: {', '.join(out['deleted']) or 'nothing was there'}",
                                f"remaining: {', '.join(out['remaining']) or 'none'}"], account, out)
            sys.exit(1 if out["remaining"] else 0)
        return
    if a.command == "update-code":                                   # after a change to the function's files
        c["lambda"].update_function_code(FunctionName=FUNCTION,
                                         ZipFile=function_zip(vendor_pydantic(architecture())))
        c["lambda"].get_waiter("function_updated_v2").wait(FunctionName=FUNCTION)
        print(f"updated {FUNCTION}")
    elif a.command == "logs":                                        # the function's own lines of the last 20 minutes
        lines = function_log(c["logs"], since_s=1200)
        emit("logs", lines or ["no lines"], account)
    elif a.command == "up":
        out = up(c)
        emit("up", [f"region {out['region']}, function architecture {out['architecture']}, zip {out['zip_bytes']} bytes",
                    out.get("smoke", ""), f"event source mapping: {json.dumps(out['mapping'])}"], account, out)
    elif a.command == "down":
        out = down(c)
        emit("down", [f"deleted: {', '.join(out['deleted']) or 'nothing was there'}",
                      f"remaining: {', '.join(out['remaining']) or 'none'}"], account, out)
        sys.exit(1 if out["remaining"] else 0)
    elif a.command == "run-race":
        r = clients.run_race(lambda: client("dynamodb"), PRICES, run_id)
        emit("run-race", clients.race_lines(r), account, r)
    elif a.command == "run-batch":
        url = c["sqs"].get_queue_url(QueueName=QUEUE)["QueueUrl"]
        clients.read_failures(c["sqs"], url, wait_s=1)               # records of an earlier run are not this run's
        written = clients.write_batch(c["dynamodb"], PRICES, run_id)
        print(f"written; the numbers appear when the 5-minute window ends (waiting up to 11 minutes) ...", flush=True)
        got = clients.wait_for_batch(c["dynamodb"], c["sqs"], WINDOWS, url, written)
        emit("run-batch", clients.batch_lines(written, got), account, {"written": written, **got})
        sys.exit(0 if got["observed"] and got["counted"] == written["good"] and len(got["failures"]) == 1 else 1)
    elif a.command == "run-load":
        ddb = client("dynamodb", max_pool_connections=max(64, a.users + 14))    # one client shared by all threads
        r = clients.run_load(ddb, PRICES, run_id, users=a.users, seconds=a.seconds, consistent=a.consistent)
        name = "run-load-consistent" if a.consistent else "run-load"
        emit(name, clients.load_lines(r, region()), account, r)


if __name__ == "__main__":
    main()
