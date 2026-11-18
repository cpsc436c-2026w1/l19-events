"""The catalog case as a Lambda function, offline: its handler on moto's Glue and S3 with the stub model."""
import json

import boto3
import pytest
from moto import mock_aws

import catalog_function
import column_drafter
from aws import catalog, deploy
from glue_catalog import GlueCatalog

REGION = "ca-central-1"


@pytest.fixture()
def c(monkeypatch, tmp_path):
    for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing", "AWS_SESSION_TOKEN": "testing",
                 "AWS_REGION": REGION, "AWS_DEFAULT_REGION": REGION}.items():
        monkeypatch.setenv(k, v)
    for k in ("AWS_PROFILE", "LAMBDA_ROLE_ARN", "COURSE_MCP_TOKEN", "KB_MCP_TOKEN", "LLM_API_KEY"):
        monkeypatch.delenv(k, raising=False)
    with mock_aws():
        cl = {**deploy.clients_for(), "s3": boto3.client("s3", region_name=REGION),
              "glue": boto3.client("glue", region_name=REGION), "events": boto3.client("events", region_name=REGION)}
        out = catalog.up(cl, REGION, "arm64", "secret-token-value", vendored=tmp_path, sleep=lambda s: None, smoke=False)
        assert "secret-token-value" not in json.dumps(out)
        monkeypatch.setenv("BUCKET", out["bucket"])
        monkeypatch.setenv("DATABASE", catalog.DATABASE)
        yield cl, out


def _event(change="UpdateTable"):
    return {"version": "0", "id": "e-1", "detail-type": "Glue Data Catalog Table State Change", "source": "aws.glue",
            "account": "123456789012", "time": "2026-11-18T05:00:00Z", "region": REGION, "resources": [],
            "detail": {"databaseName": catalog.DATABASE, "tableName": catalog.TABLE, "typeOfChange": change}}


def _call(cl, event, mode="stub"):
    return catalog_function.handler(event, None, glue=cl["glue"], s3=cl["s3"], chat=column_drafter.stub_chat(mode))


def _add(cl, *columns):
    cl["glue"].update_table(DatabaseName=catalog.DATABASE, TableInput=catalog.table_input(list(columns)))


def test_event_for_a_new_column_gives_a_pending_draft_and_does_not_write_the_catalog(c):
    cl, out = c
    assert _call(cl, {"daily": True}) == {"found_by": "daily check", "drafts": [], "model_calls": 0}   # table recorded
    _add(cl, catalog.NEW["event"])
    r = _call(cl, _event())
    (d,) = r["drafts"]
    assert (r["found_by"], d["column"], d["status"], d["attempts"], r["model_calls"]) == ("event", "on_promotion", "draft", 2, 2)
    saved = json.loads(cl["s3"].get_object(Bucket=out["bucket"], Key=d["pending_key"])["Body"].read())
    assert saved["status"] == "draft" and saved["event_id"] == "e-1" and "token" not in json.dumps(saved).lower()
    cols = cl["glue"].get_table(DatabaseName=catalog.DATABASE, Name=catalog.TABLE)["Table"]["StorageDescriptor"]["Columns"]
    assert not next(x for x in cols if x["Name"] == "on_promotion").get("Comment")
    assert _call(cl, _event())["drafts"] == []                       # the same event again: nothing new, no model call


def test_daily_check_drafts_for_a_column_no_event_announced(c):
    cl, out = c
    _call(cl, {"daily": True})
    _add(cl, catalog.NEW["event"], catalog.NEW["daily"])              # two changes, no event delivered
    r = _call(cl, {"daily": True, "max_retries": 0}, mode="stub-stubborn")
    assert r["found_by"] == "daily check" and {d["column"] for d in r["drafts"]} == {"on_promotion", "unit"}
    assert all(d["attempts"] == 1 and d["status"] == "cannot_describe" for d in r["drafts"]) and r["model_calls"] == 2
    assert _call(cl, {"daily": True})["model_calls"] == 0            # the next day: recorded, nothing to draft
    assert _call(cl, {"detail-type": "Scheduled Event", "source": "aws.events"})["found_by"] == "daily check"


def test_partition_event_and_a_malformed_event(c):
    cl, _ = c
    _call(cl, {"daily": True})
    assert _call(cl, {**_event("CreatePartition"), "detail": {**_event()["detail"], "typeOfChange": "CreatePartition",
                                                               "changedPartitions": ["2026-11"]}})["model_calls"] == 0
    with pytest.raises(ValueError, match="detail.typeOfChange"):
        _call(cl, _event("RenameTable"))
    assert catalog_function.handler({"ping": True}, None)["ok"] is True


def test_glue_catalog_has_the_interface_of_the_local_catalog(c):
    cl, out = c
    g = GlueCatalog(cl["glue"], cl["s3"], out["bucket"], catalog.DATABASE)
    t = g.get_table("", catalog.TABLE)["Table"]
    assert [x["Name"] for x in t["StorageDescriptor"]["Columns"]][:2] == ["obs_id", "product_code"] and t["VersionId"].isdigit()
    assert g.get_table("", "nope") is None and g.tables() == [catalog.TABLE]
    assert g.stored.get(catalog.TABLE) is None and list(g.stored) == []
    g.stored[catalog.TABLE] = {"Table": t}
    assert g.stored[catalog.TABLE]["Table"]["Name"] == catalog.TABLE and list(g.stored) == [catalog.TABLE]
    assert len(g.samples(catalog.TABLE, "on_promotion")) == 20 and g.samples(catalog.TABLE, "x") == []
    assert g.job_reads == ["product_code", "store", "observed_on", "price", "currency"]


def test_role_policy_and_rule(c):
    p = catalog.role_policy("111122223333", REGION)
    actions = sorted(a for s in p for a in s["Action"])
    assert actions == sorted(["glue:GetTable", "glue:GetTables", "s3:GetObject", "s3:PutObject", "s3:ListBucket",
                              "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"])
    assert not any(a.startswith("glue:") and not a.startswith("glue:Get") for a in actions)      # no write to the catalog
    assert not any("*" in a for a in actions) and catalog.ROLE.startswith("recipe-")
    puts = next(s for s in p if s["Action"] == ["s3:PutObject"])["Resource"]
    assert {r.rsplit("/", 2)[-2] for r in puts} == {"state", "pending"}
    assert catalog.event_pattern() == {"source": ["aws.glue"], "detail-type": ["Glue Data Catalog Table State Change"],
                                       "detail": {"databaseName": ["l19_catalog"]}}
    cl, out = c
    found = catalog.existing(cl, out["account"], REGION)
    assert catalog.not_ours(found) == [] and sum(v is not None for v in found.values()) >= 6
    assert cl["events"].describe_rule(Name=catalog.DAILY_RULE)["ScheduleExpression"] == "rate(1 day)"
    target = cl["events"].list_targets_by_rule(Rule=catalog.DAILY_RULE)["Targets"][0]
    assert json.loads(target["Input"]) == {"daily": True}


def test_down_leaves_nothing_and_refuses_what_is_not_ours(c):
    cl, out = c
    gone = catalog.down(cl, REGION, sleep=lambda s: None)
    assert gone["remaining"] == [] and all(v is None for v in catalog.existing(cl, out["account"], REGION).values())
    cl["events"].put_rule(Name=catalog.RULE, ScheduleExpression="rate(1 day)")       # someone else's rule, our name
    with pytest.raises(SystemExit, match="not deleted"):
        catalog.down(cl, REGION, sleep=lambda s: None)
