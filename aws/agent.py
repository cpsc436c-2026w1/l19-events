"""The agent for a held file on AWS: a bucket, a role, a Lambda function started by S3.

    uv run python -m aws.deploy agent-up      # bucket, reference files, role, function, S3 trigger (about 2 min)
    uv run python -m aws.deploy agent-run     # upload the two planted files; wait for the two results
    uv run python -m aws.deploy agent-down    # delete all of it, and check that none remains

What `agent-up` creates, tagged project=l19-events:
    l19-agent-<account>-<region>   S3 bucket: the raw prefix, the reference files, the results, the function's zip
    recipe-l19-events-agent        IAM role of the function (not created when LAMBDA_ROLE_ARN is set)
    l19-agent                      Lambda function (aws-agent/agent_function.py), timeout 600 s
    the bucket's notification      s3:ObjectCreated:* under prices_raw/ ending in .csv -> the function

The course token: `agent-up` reads COURSE_MCP_TOKEN from your environment and sets it as an environment variable
of the function. It is not printed and not saved in output/. Anyone who may read the function's configuration in
the account can read it, so delete the function when you are done (`agent-down`); a shared account would keep it
in Secrets Manager.

Documentation read 2026-11-17: strandsagents.com user guide "Deploy to AWS Lambda" (packaging for the function's
platform, handler signature); Lambda developer guide "Process Amazon S3 event notifications with Lambda" (S3
invokes the function asynchronously; S3 needs permission in the function's resource-based policy; a function that
writes to the bucket that starts it can loop, so "configure the trigger to only apply to a prefix used for
incoming objects"). Not read, taken from the boto3 service models and marked assumed: the prefix and suffix
filter rules, AddPermission with SourceArn and SourceAccount, and MaximumRetryAttempts 0 to 2 for asynchronous
invocations (default 2).
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import time
import zipfile
from pathlib import Path

from botocore.exceptions import ClientError

ROOT = Path(__file__).parent.parent
TAG = {"project": "l19-events"}
FUNCTION, ROLE = "l19-agent", "recipe-l19-events-agent"
DEPENDENCIES = ["strands-agents[openai]==1.64.0", "pydantic==2.14.3"]         # the versions in uv.lock
SOURCES = ["batch_check.py", "half_file_agent.py", "models.py", "results.py", "inputs.py", "local.py"]
ENTRY = ["aws-agent/agent_function.py", "aws-agent/s3_lake.py"]
DAY = "2026-10-31"
CASES = {"half-file": ("centre", "partial_file"), "category-missing": ("north", "category_missing")}
MODEL_CALL_BUDGET = 30            # for one agent-run: 2 files, at most 12 model calls each, no retries
TIMEOUT_S = 600                   # above the agent's deadline (240 s) plus one model call's timeout (300 s)
CONTROL = ("normal-day", "online")  # a file code settles: proves the trigger and the result path with 0 model calls


def bucket_name(account: str, region: str) -> str:
    return f"l19-agent-{account}-{region}"


def role_policy(account: str, region: str) -> list[dict]:
    """What the function's role may do: read three prefixes, write one, write its own logs."""
    b = f"arn:aws:s3:::{bucket_name(account, region)}"
    return [
        {"Effect": "Allow", "Action": ["s3:GetObject"],
         "Resource": [f"{b}/prices_raw/*", f"{b}/export-logs/*", f"{b}/reference/*"]},
        {"Effect": "Allow", "Action": ["s3:PutObject"], "Resource": [f"{b}/results/*"]},
        {"Effect": "Allow", "Resource": f"arn:aws:logs:{region}:{account}:log-group:/aws/lambda/{FUNCTION}:*",
         "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]}]


def notification(function_arn: str) -> dict:
    return {"LambdaFunctionConfigurations": [{
        "Id": "new-raw-file", "LambdaFunctionArn": function_arn, "Events": ["s3:ObjectCreated:*"],
        "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "prices_raw/"},
                                            {"Name": "suffix", "Value": ".csv"}]}}}]}


def reference_objects() -> dict[str, bytes]:
    """The files the function reads besides the held file, from the generated events."""
    e = ROOT / "events" / "batch-files"
    job = json.loads((ROOT / "events" / "manifest.json").read_text())["job"]
    return {"reference/baseline.json": (e / "baseline.json").read_bytes(),
            "reference/history.json": (e / "history.json").read_bytes(),
            "reference/products.csv": (ROOT / "seed-data" / "products.csv").read_bytes(),
            "reference/job.json": json.dumps(job).encode()}


def case_objects(case: str) -> dict[str, tuple[bytes, dict]]:
    """The held file of one case and its export log; the file carries the simulator's arrival time."""
    store, _ = CASES[case]
    folder = ROOT / "events" / "batch-files" / case
    facts = next(c for c in json.loads((ROOT / "events" / "manifest.json").read_text())["cases"] if c["id"] == case)
    arrived = facts["facts"]["per_store"][store]["arrived_at"]
    return {f"export-logs/day={DAY}/{store}.log": ((folder / "export-logs" / f"{store}.log").read_bytes(), {}),
            f"prices_raw/day={DAY}/{store}-export.csv": (
                (folder / "prices_raw" / f"day={DAY}" / f"{store}-export.csv").read_bytes(),
                {"simulated-arrival": arrived})}


def vendor(arch: str) -> Path:
    target = ROOT / "build" / "agent"
    plat = "aarch64-manylinux2014" if arch == "arm64" else "x86_64-manylinux2014"
    subprocess.run(["uv", "pip", "install", "--quiet", "--target", str(target), "--python-platform", plat,
                    "--python-version", "3.12", "--only-binary", ":all:", *DEPENDENCIES], check=True)
    return target


def function_zip(vendored: Path | None) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name in SOURCES:
            z.write(ROOT / name, name)
        for name in ENTRY:
            z.write(ROOT / name, Path(name).name)
        for path in sorted(vendored.rglob("*")) if vendored else []:
            if path.is_file() and "__pycache__" not in path.parts:
                z.write(path, path.relative_to(vendored).as_posix())
    return buf.getvalue()


# ---- what exists ----------------------------------------------------------------------------------------------

def _gone(e: ClientError) -> bool:
    return e.response["Error"]["Code"] in ("ResourceNotFoundException", "NoSuchEntity", "NoSuchBucket", "404",
                                           "NoSuchTagSet")


def existing(c: dict, account: str, region: str) -> dict[str, dict | None]:
    out: dict[str, dict | None] = {}
    b = bucket_name(account, region)
    try:
        c["s3"].head_bucket(Bucket=b)
        try:
            out[f"bucket {b}"] = {t["Key"]: t["Value"] for t in c["s3"].get_bucket_tagging(Bucket=b)["TagSet"]}
        except ClientError as e:
            if not _gone(e):
                raise
            out[f"bucket {b}"] = {}
    except ClientError as e:
        if not _gone(e):
            raise
        out[f"bucket {b}"] = None
    try:
        out[f"function {FUNCTION}"] = c["lambda"].get_function(FunctionName=FUNCTION).get("Tags", {})
    except ClientError as e:
        if not _gone(e):
            raise
        out[f"function {FUNCTION}"] = None
    try:
        out[f"role {ROLE}"] = {t["Key"]: t["Value"] for t in c["iam"].list_role_tags(RoleName=ROLE)["Tags"]}
    except ClientError as e:
        if not _gone(e):
            raise
        out[f"role {ROLE}"] = None
    groups = c["logs"].describe_log_groups(logGroupNamePrefix=f"/aws/lambda/{FUNCTION}")["logGroups"]
    out[f"log group /aws/lambda/{FUNCTION}"] = {} if any(
        g["logGroupName"] == f"/aws/lambda/{FUNCTION}" for g in groups) else None
    return out


def not_ours(found: dict) -> list[str]:
    return [n for n, tags in found.items()
            if tags is not None and not n.startswith("log group") and tags.get("project") != TAG["project"]]


# ---- up, run, down --------------------------------------------------------------------------------------------

def up(c: dict, region: str, arch: str, token: str, vendored: Path | None = None, sleep=time.sleep,
       model: str = "gateway", smoke: bool = True) -> dict:
    account = c["sts"].get_caller_identity()["Account"]
    foreign = not_ours(existing(c, account, region))
    if foreign:
        raise SystemExit(f"these names exist and are not tagged project=l19-events; nothing was changed: {foreign}")
    if model == "gateway" and not token:
        raise SystemExit("set COURSE_MCP_TOKEN first: the function needs your course token for the course LLM gateway")
    b, s3, lam, iam = bucket_name(account, region), c["s3"], c["lambda"], c["iam"]
    try:
        s3.create_bucket(Bucket=b, **({} if region == "us-east-1" else
                                      {"CreateBucketConfiguration": {"LocationConstraint": region}}))
    except ClientError as e:
        if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise
    s3.put_bucket_tagging(Bucket=b, Tagging={"TagSet": [{"Key": k, "Value": v} for k, v in TAG.items()]})
    s3.put_public_access_block(Bucket=b, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
    for key, body in reference_objects().items():
        s3.put_object(Bucket=b, Key=key, Body=body)

    role_arn = os.environ.get("LAMBDA_ROLE_ARN", "")
    if not role_arn:
        trust = {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
        try:
            role_arn = iam.create_role(RoleName=ROLE, AssumeRolePolicyDocument=json.dumps(trust),
                                       Tags=[{"Key": k, "Value": v} for k, v in TAG.items()])["Role"]["Arn"]
            sleep(10)                # a new role takes a few seconds before Lambda can assume it (assumed: 10 s)
        except iam.exceptions.EntityAlreadyExistsException:
            role_arn = iam.get_role(RoleName=ROLE)["Role"]["Arn"]
        iam.put_role_policy(RoleName=ROLE, PolicyName="least-privilege", PolicyDocument=json.dumps(
            {"Version": "2012-10-17", "Statement": role_policy(account, region)}))

    code = function_zip(vendored if vendored is not None else vendor(arch))
    s3.put_object(Bucket=b, Key="code/agent.zip", Body=code)        # over 50 MB unzipped: uploaded through S3
    env = {"BUCKET": b, "L19_AGENT_MODEL": model, **({"COURSE_MCP_TOKEN": token} if token else {}),
           **({"GATEWAY_URL": os.environ["GATEWAY_URL"]} if os.environ.get("GATEWAY_URL") else {}),
           **({"L19_MAX_MODEL_CALLS": os.environ["L19_MAX_MODEL_CALLS"]} if os.environ.get("L19_MAX_MODEL_CALLS") else {})}
    for attempt in range(6):
        try:
            lam.create_function(FunctionName=FUNCTION, Runtime="python3.12", Role=role_arn, Architectures=[arch],
                                Handler="agent_function.handler", Code={"S3Bucket": b, "S3Key": "code/agent.zip"},
                                Timeout=TIMEOUT_S, MemorySize=512, Environment={"Variables": env}, Tags=TAG)
            break
        except lam.exceptions.ResourceConflictException:
            break
        except lam.exceptions.InvalidParameterValueException as e:
            if "assume" not in str(e) or attempt == 5:
                raise
            sleep(5)
    lam.get_waiter("function_active_v2").wait(FunctionName=FUNCTION)
    function_arn = lam.get_function(FunctionName=FUNCTION)["Configuration"]["FunctionArn"]
    lam.put_function_event_invoke_config(FunctionName=FUNCTION, MaximumRetryAttempts=0)   # a failed run is not repeated
    ping = None
    if smoke:                            # a broken zip shows here, not as a missing result ten minutes later
        resp = lam.invoke(FunctionName=FUNCTION, Payload=json.dumps({"ping": True}).encode())
        ping = json.loads(resp["Payload"].read())
        if resp.get("FunctionError") or not ping.get("ok"):
            raise SystemExit(f"the function does not start: {str(ping)[:500]}")
    try:
        lam.add_permission(FunctionName=FUNCTION, StatementId="s3-new-raw-file", Action="lambda:InvokeFunction",
                           Principal="s3.amazonaws.com", SourceArn=f"arn:aws:s3:::{b}", SourceAccount=account)
    except lam.exceptions.ResourceConflictException:
        pass
    for attempt in range(8):
        try:
            s3.put_bucket_notification_configuration(Bucket=b, NotificationConfiguration=notification(function_arn))
            break
        except ClientError as e:
            # S3 checks that it may invoke the function; the permission added a moment ago is not always visible yet
            # (seen 2026-11-17: "Unable to validate the following destination configurations")
            if e.response["Error"]["Code"] != "InvalidArgument" or attempt == 7:
                raise
            sleep(5)
    return {"account": account, "region": region, "bucket": b, "architecture": arch, "zip_bytes": len(code),
            "role_arn": role_arn, "model": model, "timeout_s": TIMEOUT_S, "ping": ping}


def _result(s3, b: str, store: str) -> dict | None:
    try:
        return json.loads(s3.get_object(Bucket=b, Key=f"results/day={DAY}/{store}.json")["Body"].read())
    except ClientError as e:
        if not _gone(e) and e.response["Error"]["Code"] != "NoSuchKey":
            raise
        return None


def run(c: dict, region: str, deadline_s: int = 660, poll_s: int = 10, sleep=time.sleep, clock=time.time,
        control_deadline_s: int = 360) -> dict:
    """First a file that code settles: its result must arrive with 0 model calls, which proves the trigger, the
    key and the write of the result. Only then the two planted files (each starts the function); wait for both."""
    account = c["sts"].get_caller_identity()["Account"]
    b, s3 = bucket_name(account, region), c["s3"]
    for store in [s for s, _ in CASES.values()] + [CONTROL[1]]:     # yesterday's answers are not today's
        s3.delete_object(Bucket=b, Key=f"results/day={DAY}/{store}.json")
    started = clock()
    case, store = CONTROL
    facts = next(x for x in json.loads((ROOT / "events" / "manifest.json").read_text())["cases"] if x["id"] == case)
    body = (ROOT / "events" / "batch-files" / case / "prices_raw" / f"day={DAY}" / f"{store}-export.csv").read_bytes()
    control, uploads = None, 0
    while control is None and clock() - started < control_deadline_s:
        # Seen 2026-11-17: a file uploaded a few seconds after the notification was configured started nothing; the
        # same upload minutes later did. So the control file is uploaded again every 30 s until its result is there.
        s3.put_object(Bucket=b, Key=f"prices_raw/day={DAY}/{store}-export.csv", Body=body,
                      Metadata={"simulated-arrival": facts["facts"]["per_store"][store]["arrived_at"]})
        uploads += 1
        for _ in range(6):
            control = _result(s3, b, store)
            if control is not None:
                break
            sleep(5)
    if control is None or control["model_calls"] != 0 or control["decided_by"] != "code":
        return {"waited_s": round(clock() - started), "control": control, "results": {}, "model_calls": 0,
                "budget": MODEL_CALL_BUDGET, "stopped": "the control file did not come back settled by code; "
                                                        "the planted files were not uploaded"}
    for case in CASES:
        objects = case_objects(case)
        for key in sorted(objects):                                  # export-logs/ before prices_raw/: the log is there first
            body, meta = objects[key]
            s3.put_object(Bucket=b, Key=key, Body=body, Metadata=meta)
    results: dict[str, dict] = {}
    while len(results) < len(CASES) and clock() - started < deadline_s:
        for case, (store, _) in CASES.items():
            if case not in results and (got := _result(s3, b, store)) is not None:
                results[case] = got
        if len(results) < len(CASES):
            sleep(poll_s)
    return {"waited_s": round(clock() - started), "control": control, "results": results,
            "model_calls": sum(r["model_calls"] for r in results.values()), "budget": MODEL_CALL_BUDGET}


def durations(logs, since_epoch: float) -> dict[str, float]:
    """Lambda's own duration of each invocation, by request id, from the REPORT lines of the function's log."""
    out = {}
    try:
        events = logs.filter_log_events(logGroupName=f"/aws/lambda/{FUNCTION}", filterPattern="REPORT",
                                        startTime=int(since_epoch * 1000))["events"]
    except ClientError:
        return out
    for e in events:
        if "RequestId:" not in e["message"] or "Duration:" not in e["message"]:
            continue
        parts = e["message"].split("\t")
        rid = parts[0].split("RequestId:")[1].strip()
        out[rid] = float(next(p for p in parts if p.startswith("Duration:")).split()[1]) / 1000
    return out


def run_lines(r: dict, lambda_s: dict[str, float]) -> list[str]:
    lines, ctl = [], r.get("control")
    lines.append(f"control ({CONTROL[0]}, {CONTROL[1]}): " + (
        f"code {ctl['code_verdict']}, decided by {ctl['decided_by']}, {ctl['model_calls']} model calls; "
        f"S3 sent the key as {ctl.get('event_key')}" if ctl else "NO RESULT"))
    if r.get("stopped"):
        return lines + [f"STOPPED: {r['stopped']}"]
    for case, (store, planted) in CASES.items():
        x = r["results"].get(case)
        if not x:
            lines.append(f"{case}: NO RESULT after {r['waited_s']} s")
            continue
        took = lambda_s.get(x.get("request_id", ""))
        lines.append(f"{case} ({store}): code {x['code_verdict']}; agent named {x['cause']} (planted {planted}), "
                     f"verified {x['verified']}, fix {x['fix']}; {x['model_calls']} model calls, {x['tool_calls']} tool "
                     f"calls, {x['seconds']} s in the handler, Lambda duration "
                     f"{f'{took:.1f} s' if took else 'not in the log yet'}{'; stopped: ' + x['stopped'] if x['stopped'] else ''}")
    lines.append(f"model calls: {r['model_calls']} of at most {r['budget']}")
    return lines


def down(c: dict, region: str, sleep=time.sleep) -> dict:
    account = c["sts"].get_caller_identity()["Account"]
    found = existing(c, account, region)
    foreign = not_ours(found)
    if foreign:
        raise SystemExit(f"not tagged project=l19-events, so not deleted: {foreign}")
    b, s3, lam, iam = bucket_name(account, region), c["s3"], c["lambda"], c["iam"]
    if found[f"function {FUNCTION}"] is not None:
        lam.delete_function(FunctionName=FUNCTION)                  # its permission for S3 and its token go with it
    if found[f"bucket {b}"] is not None:
        s3.put_bucket_notification_configuration(Bucket=b, NotificationConfiguration={})
        while True:
            page = s3.list_objects_v2(Bucket=b)
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if not keys:
                break
            s3.delete_objects(Bucket=b, Delete={"Objects": keys})
        s3.delete_bucket(Bucket=b)
    if found[f"log group /aws/lambda/{FUNCTION}"] is not None:
        c["logs"].delete_log_group(logGroupName=f"/aws/lambda/{FUNCTION}")
    if found[f"role {ROLE}"] is not None and not os.environ.get("LAMBDA_ROLE_ARN"):
        for policy in iam.list_role_policies(RoleName=ROLE)["PolicyNames"]:
            iam.delete_role_policy(RoleName=ROLE, PolicyName=policy)
        iam.delete_role(RoleName=ROLE)
    left: dict = {}
    for _ in range(20):
        left = {k: v for k, v in existing(c, account, region).items() if v is not None}
        if os.environ.get("LAMBDA_ROLE_ARN"):
            left.pop(f"role {ROLE}", None)
        if not left:
            break
        if all(k.startswith("log group") for k in left):             # Lambda wrote its last lines after the delete
            try:
                c["logs"].delete_log_group(logGroupName=f"/aws/lambda/{FUNCTION}")
            except ClientError:
                pass
        sleep(5)
    return {"deleted": [k for k, v in found.items() if v is not None], "remaining": sorted(left)}
