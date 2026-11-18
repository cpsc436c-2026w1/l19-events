"""The metadata case on AWS: a Glue table changes, EventBridge starts a function, a description is drafted.

    uv run python -m aws.deploy catalog-up      # Glue database and table, bucket, role, function, two rules
    uv run python -m aws.deploy catalog-run     # add a column (event), then one with the rule off (daily check)
    uv run python -m aws.deploy catalog-down    # delete all of it, and check that none remains

What `catalog-up` creates, tagged project=l19-events where the service has tags:
    l19_catalog, l19_catalog.l19_prices_raw   Glue database and table (the table itself carries no tag)
    l19-catalog-<account>-<region>            S3 bucket: stored definitions, samples, pending drafts, the zip
    recipe-l19-events-catalog                 IAM role of the function (not created when LAMBDA_ROLE_ARN is set)
    l19-catalog                               Lambda function (aws-catalog/catalog_function.py)
    l19-catalog-change                        EventBridge rule: Glue table changes in l19_catalog -> the function
    l19-catalog-daily                         EventBridge rule: once a day -> the function with {"daily": true}

Documentation: Glue developer guide, automating-awsglue-with-cloudwatch-events (read 2026-11-17): the event
"Glue Data Catalog Table State Change" for UpdateTable, with typeOfChange, databaseName, tableName, and best-effort
delivery. EventBridge user guide, "Using resource-based policies for Amazon EventBridge", AWS Lambda permissions:
a rule may invoke a function when the function's policy allows events.amazonaws.com with the rule's ARN as source
(found by search on 2026-11-17, the page was not read in full; the event pattern and target shapes are from the
boto3 service model).

The course token is handled as in aws/agent.py: read from your environment at `catalog-up`, set as an environment
variable of the function, never printed or saved; `catalog-down` removes it with the function.
"""
from __future__ import annotations

import io
import json
import os
import time
import zipfile
from pathlib import Path

from botocore.exceptions import ClientError

from aws import agent

ROOT = Path(__file__).parent.parent
TAG = {"project": "l19-events"}
FUNCTION, ROLE = "l19-catalog", "recipe-l19-events-catalog"
DATABASE, TABLE = "l19_catalog", "l19_prices_raw"
RULE, DAILY_RULE = "l19-catalog-change", "l19-catalog-daily"
SOURCES = ["catalog_watch.py", "column_drafter.py", "models.py", "results.py", "inputs.py", "local.py"]
ENTRY = ["aws-catalog/catalog_function.py", "aws-catalog/glue_catalog.py"]
COLUMNS = [("obs_id", "string", "id of the observation"), ("product_code", "string", "product code"),
           ("store", "string", "north, centre or online"), ("observed_on", "date", "day of the observation"),
           ("price", "double", "price in the store's currency"), ("currency", "string", "CAD or EUR"),
           ("source", "string", "receipt reference")]
# the two columns the run adds: the first is announced by an event, the second while the rule is off
NEW = {"event": ("on_promotion", "boolean"), "daily": ("unit", "string")}
UNIT_SAMPLES = ["each", "kg", "each", "each", "100 g", "kg", "each", "L", "each", "kg"]
MODEL_CALL_BUDGET = 4              # one catalog-run: up to 3 calls for the event's column, 1 for the daily check's


def bucket_name(account: str, region: str) -> str:
    return f"l19-catalog-{account}-{region}"


def event_pattern() -> dict:
    return {"source": ["aws.glue"], "detail-type": ["Glue Data Catalog Table State Change"],
            "detail": {"databaseName": [DATABASE]}}


def role_policy(account: str, region: str) -> list[dict]:
    b = f"arn:aws:s3:::{bucket_name(account, region)}"
    g = f"arn:aws:glue:{region}:{account}"
    return [
        {"Effect": "Allow", "Action": ["glue:GetTable", "glue:GetTables"],      # read the catalog; no glue write
         "Resource": [f"{g}:catalog", f"{g}:database/{DATABASE}", f"{g}:table/{DATABASE}/*"]},
        {"Effect": "Allow", "Action": ["s3:GetObject"],
         "Resource": [f"{b}/state/*", f"{b}/samples/*", f"{b}/reference/*"]},
        {"Effect": "Allow", "Action": ["s3:PutObject"], "Resource": [f"{b}/state/*", f"{b}/pending/*"]},
        {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": [b],       # so a missing object reads as missing
         "Condition": {"StringLike": {"s3:prefix": ["state/*", "samples/*"]}}},
        {"Effect": "Allow", "Resource": f"arn:aws:logs:{region}:{account}:log-group:/aws/lambda/{FUNCTION}:*",
         "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]}]


def table_input(extra: list[tuple[str, str]] = ()) -> dict:
    cols = [{"Name": n, "Type": t, "Comment": c} for n, t, c in COLUMNS] + [{"Name": n, "Type": t} for n, t in extra]
    return {"Name": TABLE, "TableType": "EXTERNAL_TABLE", "Parameters": {"classification": "csv"},
            "StorageDescriptor": {"Columns": cols, "Location": "s3://l19-catalog-no-data/prices_raw/",
                                  "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                                  "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                                  "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"}}}


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


def _code(e: ClientError) -> str:
    return e.response["Error"]["Code"]


GONE = ("ResourceNotFoundException", "NoSuchEntity", "NoSuchBucket", "404", "NoSuchTagSet", "EntityNotFoundException")


def existing(c: dict, account: str, region: str) -> dict[str, dict | None]:
    out: dict[str, dict | None] = {}
    b = bucket_name(account, region)

    def look(name, fn):
        try:
            out[name] = fn()
        except ClientError as e:
            if _code(e) not in GONE:
                raise
            out[name] = None

    def bucket_tags():
        c["s3"].head_bucket(Bucket=b)
        try:
            return {t["Key"]: t["Value"] for t in c["s3"].get_bucket_tagging(Bucket=b)["TagSet"]}
        except ClientError as e:
            if _code(e) not in GONE:
                raise
            return {}

    look(f"bucket {b}", bucket_tags)
    look(f"glue database {DATABASE}", lambda: (c["glue"].get_database(Name=DATABASE), c["glue"].get_tags(
        ResourceArn=f"arn:aws:glue:{region}:{account}:database/{DATABASE}")["Tags"])[1])
    look(f"function {FUNCTION}", lambda: c["lambda"].get_function(FunctionName=FUNCTION).get("Tags", {}))
    look(f"role {ROLE}", lambda: {t["Key"]: t["Value"] for t in c["iam"].list_role_tags(RoleName=ROLE)["Tags"]})
    for rule in (RULE, DAILY_RULE):
        look(f"rule {rule}", lambda rule=rule: {t["Key"]: t["Value"] for t in c["events"].list_tags_for_resource(
            ResourceARN=c["events"].describe_rule(Name=rule)["Arn"])["Tags"]})
    groups = c["logs"].describe_log_groups(logGroupNamePrefix=f"/aws/lambda/{FUNCTION}")["logGroups"]
    out[f"log group /aws/lambda/{FUNCTION}"] = {} if any(
        g["logGroupName"] == f"/aws/lambda/{FUNCTION}" for g in groups) else None
    return out


def not_ours(found: dict) -> list[str]:
    return [n for n, tags in found.items()
            if tags is not None and not n.startswith("log group") and tags.get("project") != TAG["project"]]


def up(c: dict, region: str, arch: str, token: str, vendored: Path | None = None, sleep=time.sleep,
       model: str = "gateway", smoke: bool = True) -> dict:
    account = c["sts"].get_caller_identity()["Account"]
    foreign = not_ours(existing(c, account, region))
    if foreign:
        raise SystemExit(f"these names exist and are not tagged project=l19-events; nothing was changed: {foreign}")
    if model == "gateway" and not token:
        raise SystemExit("set COURSE_MCP_TOKEN first: the function needs your course token for the course LLM gateway")
    b, s3, lam, iam, glue, ev = bucket_name(account, region), c["s3"], c["lambda"], c["iam"], c["glue"], c["events"]
    tags = [{"Key": k, "Value": v} for k, v in TAG.items()]
    try:
        s3.create_bucket(Bucket=b, **({} if region == "us-east-1" else
                                      {"CreateBucketConfiguration": {"LocationConstraint": region}}))
    except ClientError as e:
        if _code(e) != "BucketAlreadyOwnedByYou":
            raise
    s3.put_bucket_tagging(Bucket=b, Tagging={"TagSet": tags})
    s3.put_public_access_block(Bucket=b, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
    job = json.loads((ROOT / "events" / "manifest.json").read_text())["job"]
    s3.put_object(Bucket=b, Key="reference/job.json", Body=json.dumps(job).encode())
    promo = json.loads((ROOT / "events" / "catalog-changes" / "new-column" / "samples.json").read_text())["values"]
    for column, values in ((NEW["event"][0], promo), (NEW["daily"][0], UNIT_SAMPLES)):
        s3.put_object(Bucket=b, Key=f"samples/{TABLE}.{column}.json", Body=json.dumps({"values": values}).encode())
    try:
        glue.create_database(DatabaseInput={"Name": DATABASE, "Description": "CPSC 436C L19 catalog demo"}, Tags=TAG)
    except glue.exceptions.AlreadyExistsException:
        pass
    try:
        glue.create_table(DatabaseName=DATABASE, TableInput=table_input())
    except glue.exceptions.AlreadyExistsException:
        pass

    role_arn = os.environ.get("LAMBDA_ROLE_ARN", "")
    if not role_arn:
        trust = {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
        try:
            role_arn = iam.create_role(RoleName=ROLE, AssumeRolePolicyDocument=json.dumps(trust), Tags=tags)["Role"]["Arn"]
            sleep(10)
        except iam.exceptions.EntityAlreadyExistsException:
            role_arn = iam.get_role(RoleName=ROLE)["Role"]["Arn"]
        iam.put_role_policy(RoleName=ROLE, PolicyName="least-privilege", PolicyDocument=json.dumps(
            {"Version": "2012-10-17", "Statement": role_policy(account, region)}))

    code = function_zip(vendored if vendored is not None else agent.vendor(arch))
    s3.put_object(Bucket=b, Key="code/catalog.zip", Body=code)
    env = {"BUCKET": b, "DATABASE": DATABASE, "L19_DRAFT_MODEL": model, **({"COURSE_MCP_TOKEN": token} if token else {}),
           **({"GATEWAY_URL": os.environ["GATEWAY_URL"]} if os.environ.get("GATEWAY_URL") else {})}
    for attempt in range(6):
        try:
            lam.create_function(FunctionName=FUNCTION, Runtime="python3.12", Role=role_arn, Architectures=[arch],
                                Handler="catalog_function.handler", Code={"S3Bucket": b, "S3Key": "code/catalog.zip"},
                                Timeout=900, MemorySize=512, Environment={"Variables": env}, Tags=TAG)
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
    if smoke:
        resp = lam.invoke(FunctionName=FUNCTION, Payload=json.dumps({"ping": True}).encode())
        ping = json.loads(resp["Payload"].read())
        if resp.get("FunctionError") or not ping.get("ok"):
            raise SystemExit(f"the function does not start: {str(ping)[:500]}")
    for rule, kw in ((RULE, {"EventPattern": json.dumps(event_pattern())}),
                     (DAILY_RULE, {"ScheduleExpression": "rate(1 day)"})):
        arn = ev.put_rule(Name=rule, State="ENABLED", Tags=tags, **kw)["RuleArn"]
        target = {"Id": "drafter", "Arn": function_arn}
        if rule == DAILY_RULE:
            target["Input"] = json.dumps({"daily": True})
        ev.put_targets(Rule=rule, Targets=[target])
        try:
            lam.add_permission(FunctionName=FUNCTION, StatementId=f"events-{rule}", Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com", SourceArn=arn)
        except lam.exceptions.ResourceConflictException:
            pass
    return {"account": account, "region": region, "bucket": b, "architecture": arch, "zip_bytes": len(code),
            "model": model, "ping": ping, "database": DATABASE, "table": TABLE}


def _pending(s3, b: str, column: str) -> dict | None:
    try:
        o = s3.get_object(Bucket=b, Key=f"pending/{TABLE}.{column}.json")
    except ClientError as e:
        if _code(e) != "NoSuchKey":
            raise
        return None
    return {**json.loads(o["Body"].read()), "written_at": o["LastModified"].timestamp()}


def run(c: dict, region: str, deadline_s: int = 420, sleep=time.sleep, clock=time.time) -> dict:
    """1. The daily check records the table as it is (no draft: nothing is new). 2. UpdateTable adds a column; the
    event starts the function; wait for the pending draft. 3. With the rule off, UpdateTable adds another column:
    no event; the daily check finds it and drafts (one model call, no retry)."""
    account = c["sts"].get_caller_identity()["Account"]
    b, s3, lam, glue, ev = bucket_name(account, region), c["s3"], c["lambda"], c["glue"], c["events"]

    def invoke(payload: dict) -> dict:
        resp = lam.invoke(FunctionName=FUNCTION, Payload=json.dumps(payload).encode())
        body = json.loads(resp["Payload"].read())
        if resp.get("FunctionError"):
            raise SystemExit(f"the function failed: {str(body)[:500]}")
        return body

    for column, _ in NEW.values():
        s3.delete_object(Bucket=b, Key=f"pending/{TABLE}.{column}.json")
    glue.update_table(DatabaseName=DATABASE, TableInput=table_input())             # back to the first columns
    s3.delete_object(Bucket=b, Key=f"state/{TABLE}.json")
    first = invoke({"daily": True, "max_retries": 0})
    out = {"first_daily_check": {"drafts": len(first["drafts"]), "model_calls": first["model_calls"]}}
    if first["model_calls"]:
        return {**out, "stopped": "the first daily check called the model; nothing more was run"}

    ev.enable_rule(Name=RULE)
    sleep(30)                                         # a new rule needs a moment before it matches (assumed: 30 s)
    t0 = clock()
    glue.update_table(DatabaseName=DATABASE, TableInput=table_input([NEW["event"]]))
    draft = None
    while draft is None and clock() - t0 < deadline_s:
        draft = _pending(s3, b, NEW["event"][0])
        if draft is None:
            sleep(3)
    out["event"] = {"column": NEW["event"][0], "arrived": draft is not None,
                    "seconds_from_update_table_to_draft": round(draft["written_at"] - t0, 1) if draft else None,
                    "draft": draft}
    calls = draft["model_calls"] if draft else 0

    ev.disable_rule(Name=RULE)                        # the next change sends no event to the function
    sleep(15)
    glue.update_table(DatabaseName=DATABASE, TableInput=table_input([NEW["event"], NEW["daily"]]))
    sleep(20)
    missed = _pending(s3, b, NEW["daily"][0]) is None
    daily = invoke({"daily": True, "max_retries": 0})
    ev.enable_rule(Name=RULE)
    d = next((x for x in daily["drafts"] if x["column"] == NEW["daily"][0]), None)
    out["daily"] = {"column": NEW["daily"][0], "no_event_reached_the_function": missed, "draft": d,
                    "drafts": len(daily["drafts"]), "model_calls": daily["model_calls"]}
    table = glue.get_table(DatabaseName=DATABASE, Name=TABLE)["Table"]
    out["catalog_comments_written"] = sum(bool(col.get("Comment")) for col in table["StorageDescriptor"]["Columns"]
                                          if col["Name"] in (NEW["event"][0], NEW["daily"][0]))
    out["model_calls"], out["budget"] = calls + daily["model_calls"], MODEL_CALL_BUDGET
    return out


def run_lines(r: dict) -> list[str]:
    lines = [f"first daily check: table recorded, {r['first_daily_check']['drafts']} drafts, "
             f"{r['first_daily_check']['model_calls']} model calls"]
    if r.get("stopped"):
        return lines + [f"STOPPED: {r['stopped']}"]
    e, d = r["event"], r["daily"]
    if e["arrived"]:
        x = e["draft"]
        lines.append(f"event: UpdateTable added {e['column']}; draft written {e['seconds_from_update_table_to_draft']} s "
                     f"later; status {x['status']}, valid on try {x['attempts']}, {x['model_calls']} model calls")
        lines.append(f"  description: {x['description']}")
    else:
        lines.append(f"event: UpdateTable added {e['column']}; NO DRAFT before the deadline (the event did not arrive)")
    x = d["draft"]
    lines.append(f"daily check: {d['column']} added with the rule off (no event reached the function: "
                 f"{d['no_event_reached_the_function']}); " + (
                     f"found by the daily check; status {x['status']}, valid on try {x['attempts']}, "
                     f"{d['model_calls']} model calls" if x else "NOT FOUND by the daily check"))
    if x and x["description"]:
        lines.append(f"  description: {x['description']}")
    lines.append(f"comments written to the Glue Data Catalog: {r['catalog_comments_written']}; "
                 f"model calls: {r['model_calls']} of at most {r['budget']}")
    return lines


def down(c: dict, region: str, sleep=time.sleep) -> dict:
    account = c["sts"].get_caller_identity()["Account"]
    found = existing(c, account, region)
    foreign = not_ours(found)
    if foreign:
        raise SystemExit(f"not tagged project=l19-events, so not deleted: {foreign}")
    b, s3, lam, iam, glue, ev = bucket_name(account, region), c["s3"], c["lambda"], c["iam"], c["glue"], c["events"]
    for rule in (RULE, DAILY_RULE):
        if found[f"rule {rule}"] is not None:
            ev.remove_targets(Rule=rule, Ids=["drafter"])
            ev.delete_rule(Name=rule)
    if found[f"function {FUNCTION}"] is not None:
        lam.delete_function(FunctionName=FUNCTION)
    if found[f"glue database {DATABASE}"] is not None:
        glue.delete_database(Name=DATABASE)                          # its tables go with it
    if found[f"bucket {b}"] is not None:
        while True:
            keys = [{"Key": o["Key"]} for o in s3.list_objects_v2(Bucket=b).get("Contents", [])]
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
        sleep(5)
    return {"deleted": [k for k, v in found.items() if v is not None], "remaining": sorted(left)}
