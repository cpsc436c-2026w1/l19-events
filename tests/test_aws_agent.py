"""The agent as a Lambda function, offline: its handler on moto's S3 with the stub model, and the deploy config."""
import io
import json
import zipfile

import boto3
import pytest
from moto import mock_aws

import agent_function
import half_file_agent
from aws import agent, deploy
from s3_lake import S3Lake

REGION = "ca-central-1"


@pytest.fixture()
def c(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing", "AWS_SESSION_TOKEN": "testing",
                 "AWS_REGION": REGION, "AWS_DEFAULT_REGION": REGION}.items():
        monkeypatch.setenv(k, v)
    for k in ("AWS_PROFILE", "LAMBDA_ROLE_ARN", "COURSE_MCP_TOKEN", "KB_MCP_TOKEN", "LLM_API_KEY"):
        monkeypatch.delenv(k, raising=False)
    with mock_aws():
        yield {**deploy.clients_for(), "s3": boto3.client("s3", region_name=REGION)}


def _bucket(c, cases=("half-file", "category-missing")):
    account = c["sts"].get_caller_identity()["Account"]
    b = agent.bucket_name(account, REGION)
    c["s3"].create_bucket(Bucket=b, CreateBucketConfiguration={"LocationConstraint": REGION})
    for key, body in agent.reference_objects().items():
        c["s3"].put_object(Bucket=b, Key=key, Body=body)
    for case in cases:
        for key, (body, meta) in agent.case_objects(case).items():
            c["s3"].put_object(Bucket=b, Key=key, Body=body, Metadata=meta)
    return b


def _event(store):
    return {"Records": [{"eventTime": "2026-11-18T03:00:00.000Z", "eventName": "ObjectCreated:Put",
                         "s3": {"object": {"key": f"prices_raw/day%3D2026-10-31/{store}-export.csv"}}}]}


# ---- the handler --------------------------------------------------------------------------------------------

@pytest.mark.parametrize("case", list(agent.CASES))
def test_handler_runs_the_runbook_then_the_agent_and_writes_the_result(c, case, monkeypatch):
    b = _bucket(c)
    monkeypatch.setenv("BUCKET", b)
    store, planted = agent.CASES[case]
    (out,) = agent_function.handler(_event(store), None, s3=c["s3"], model=half_file_agent.stub_model())
    assert (out["code_verdict"], out["decided_by"], out["cause"], out["verified"]) == ("unsettled", "agent", planted, True)
    assert out["arrived_at"].startswith("2026-10-31T01:4")          # the simulator's arrival, not the upload time
    assert out["model_calls"] <= 12 and out["tool_calls"] <= 10 and out["tools_called"]
    saved = json.loads(c["s3"].get_object(Bucket=b, Key=f"results/day=2026-10-31/{store}.json")["Body"].read())
    assert saved["cause"] == planted and saved["fix"] == "ask_upstream"
    assert "token" not in json.dumps(saved).lower()


def test_handler_does_not_call_the_model_for_a_file_code_settles(c, monkeypatch):
    b = _bucket(c, cases=())
    monkeypatch.setenv("BUCKET", b)
    data = (agent.ROOT / "events/batch-files/normal-day/prices_raw/day=2026-10-31/north-export.csv").read_bytes()
    c["s3"].put_object(Bucket=b, Key="prices_raw/day=2026-10-31/north-export.csv", Body=data,
                       Metadata={"simulated-arrival": "2026-10-31T01:40:10Z"})
    (out,) = agent_function.handler(_event("north"), None, s3=c["s3"], model=half_file_agent.stub_model("stub-runaway"))
    assert (out["code_verdict"], out["decided_by"], out["model_calls"]) == ("pass", "code", 0)


def test_runaway_model_is_stopped_below_the_function_timeout(c, monkeypatch):
    b = _bucket(c)
    monkeypatch.setenv("BUCKET", b)
    (out,) = agent_function.handler(_event("centre"), None, s3=c["s3"], model=half_file_agent.stub_model("stub-runaway"))
    assert (out["model_calls"], out["tool_calls"]) == (12, 10) and out["stopped"] and out["fix"] == "escalate"
    assert agent_function.DEADLINE_S < agent.TIMEOUT_S


def test_s3_lake_has_the_interface_of_the_local_lake(c):
    b = _bucket(c)
    lake = S3Lake(c["s3"], b)
    assert len(lake.rows_per_product("centre", "2026-10-31")) == 205 and len(lake.rows_per_product("centre", "2026-10-30")) == 411
    assert lake.rows_per_product("online", "2026-10-31") is None and lake.export_log("online") == []
    assert len(lake.export_log("centre")) == 3 and len(lake.product_categories()) == 26
    assert lake.job_started_at() == "2026-10-31T02:00:04Z" and lake.baseline("north")["usual"]["rows"] > 400


# ---- the deploy configuration -------------------------------------------------------------------------------

def test_role_policy_and_trigger():
    p = agent.role_policy("111122223333", REGION)
    assert sorted(a for s in p for a in s["Action"]) == sorted(
        ["s3:GetObject", "s3:PutObject", "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"])
    read, write = p[0]["Resource"], p[1]["Resource"]
    assert all(r.startswith("arn:aws:s3:::l19-agent-111122223333-ca-central-1/") for r in read + write)
    assert {r.rsplit("/", 2)[-2] for r in read} == {"prices_raw", "export-logs", "reference"}
    assert [r.rsplit("/", 2)[-2] for r in write] == ["results"] and not set(read) & set(write)
    assert not any("*" in a for s in p for a in s["Action"]) and agent.ROLE.startswith("recipe-")
    rules = agent.notification("arn:f")["LambdaFunctionConfigurations"][0]
    assert rules["Events"] == ["s3:ObjectCreated:*"]
    assert {r["Name"]: r["Value"] for r in rules["Filter"]["Key"]["FilterRules"]} == {"prefix": "prices_raw/", "suffix": ".csv"}
    assert 2 * half_file_agent.MAX_MODEL_CALLS <= agent.MODEL_CALL_BUDGET <= 30


def test_zip_holds_the_unchanged_code(tmp_path):
    names = set(zipfile.ZipFile(io.BytesIO(agent.function_zip(tmp_path))).namelist())
    assert names == {"batch_check.py", "half_file_agent.py", "models.py", "results.py", "inputs.py", "local.py",
                     "agent_function.py", "s3_lake.py"}


def test_up_run_down_on_moto(c, tmp_path):
    with pytest.raises(SystemExit, match="COURSE_MCP_TOKEN"):
        agent.up(c, REGION, "arm64", "", vendored=tmp_path, sleep=lambda s: None, smoke=False)
    out = agent.up(c, REGION, "arm64", "secret-token-value", vendored=tmp_path, sleep=lambda s: None, smoke=False)
    assert "secret-token-value" not in json.dumps(out)              # the token is not in what `up` prints or saves
    found = agent.existing(c, out["account"], REGION)
    assert agent.not_ours(found) == [] and sum(v is not None for v in found.values()) >= 3
    cfg = c["lambda"].get_function_configuration(FunctionName=agent.FUNCTION)
    assert cfg["Timeout"] == 300 and cfg["Environment"]["Variables"]["BUCKET"] == out["bucket"]
    assert c["lambda"].get_function_event_invoke_config(FunctionName=agent.FUNCTION)["MaximumRetryAttempts"] == 0
    note = c["s3"].get_bucket_notification_configuration(Bucket=out["bucket"])["LambdaFunctionConfigurations"][0]
    assert note["Id"] == "new-raw-file"
    # the run: uploads, then (here) results written by hand, as the function would
    ticks = iter(range(0, 10000, 5))
    r = agent.run(c, REGION, deadline_s=20, sleep=lambda s: None, clock=lambda: next(ticks))
    assert r["results"] == {} and any("NO RESULT" in line for line in agent.run_lines(r, {}))
    keys = [o["Key"] for o in c["s3"].list_objects_v2(Bucket=out["bucket"])["Contents"]]
    assert "prices_raw/day=2026-10-31/centre-export.csv" in keys and "export-logs/day=2026-10-31/north.log" in keys
    head = c["s3"].head_object(Bucket=out["bucket"], Key="prices_raw/day=2026-10-31/centre-export.csv")
    assert head["Metadata"]["simulated-arrival"] == "2026-10-31T01:41:10Z"
    gone = agent.down(c, REGION, sleep=lambda s: None)
    assert gone["remaining"] == [] and all(v is None for v in agent.existing(c, out["account"], REGION).values())


def test_up_refuses_a_bucket_that_is_not_ours(c, tmp_path):
    account = c["sts"].get_caller_identity()["Account"]
    c["s3"].create_bucket(Bucket=agent.bucket_name(account, REGION), CreateBucketConfiguration={"LocationConstraint": REGION})
    with pytest.raises(SystemExit, match="not tagged"):
        agent.up(c, REGION, "arm64", "t", vendored=tmp_path, sleep=lambda s: None, smoke=False)
    with pytest.raises(SystemExit, match="not deleted"):
        agent.down(c, REGION, sleep=lambda s: None)


def test_ping_checks_the_imports_without_a_model_call(monkeypatch):
    monkeypatch.delenv("COURSE_MCP_TOKEN", raising=False)
    assert agent_function.handler({"ping": True}, None) == {"ok": True, "strands": agent_function.handler(
        {"ping": True}, None)["strands"], "token_set": False}
