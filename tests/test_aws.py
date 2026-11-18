"""The AWS runs, offline: the deploy configuration, and the clients against moto. No real AWS call."""
import io
import json
import zipfile

import boto3
import pytest
from moto import mock_aws

from aws import clients, deploy


@pytest.fixture()
def aws(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing", "AWS_SESSION_TOKEN": "testing",
                 "AWS_REGION": "ca-central-1", "AWS_DEFAULT_REGION": "ca-central-1"}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("LAMBDA_ROLE_ARN", raising=False)
    with mock_aws():
        yield deploy.clients_for()


def _tables(c):
    """Only the two tables, for the client tests."""
    for name, (h, r) in ((deploy.PRICES, ("product_code", "store")), (deploy.WINDOWS, ("pk", "sk"))):
        c["dynamodb"].create_table(TableName=name, BillingMode="PAY_PER_REQUEST",
                                   KeySchema=[{"AttributeName": h, "KeyType": "HASH"}, {"AttributeName": r, "KeyType": "RANGE"}],
                                   AttributeDefinitions=[{"AttributeName": h, "AttributeType": "S"},
                                                         {"AttributeName": r, "AttributeType": "S"}])


# ---- configuration ------------------------------------------------------------------------------------------

def test_mapping_has_the_settings_of_the_failing_batch_case():
    m = deploy.mapping_config("arn:stream", "arn:queue")
    assert m["TumblingWindowInSeconds"] == 300 and 0 <= m["TumblingWindowInSeconds"] <= 900     # services-ddb-params
    assert m["BisectBatchOnFunctionError"] is True and 0 <= m["MaximumRetryAttempts"] <= 3
    assert m["DestinationConfig"] == {"OnFailure": {"Destination": "arn:queue"}}
    assert m["StartingPosition"] == "TRIM_HORIZON" and "FunctionResponseTypes" not in m


def test_role_policy_is_least_privilege(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "ca-central-1")
    p = deploy.role_policy("111122223333", "arn:aws:dynamodb:ca-central-1:111122223333:table/l19-prices/stream/x")
    actions = sorted(a for s in p for a in s["Action"])
    assert actions == sorted(["dynamodb:GetRecords", "dynamodb:GetShardIterator", "dynamodb:DescribeStream",
                              "dynamodb:ListStreams", "dynamodb:PutItem", "dynamodb:Query", "sqs:SendMessage",
                              "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"])
    for s in p:
        assert s["Effect"] == "Allow" and isinstance(s["Resource"], str) and "l19-" in s["Resource"]
        assert not any("*" in a for a in s["Action"])
        assert s["Resource"].endswith(":*") == s["Resource"].startswith("arn:aws:logs")     # only the log streams
    assert deploy.ROLE.startswith("recipe-")


def test_names_and_zip(tmp_path):
    assert all(n.startswith("l19-") for n in (deploy.PRICES, deploy.WINDOWS, deploy.QUEUE, deploy.FUNCTION))
    (tmp_path / "pydantic").mkdir()
    (tmp_path / "pydantic" / "__init__.py").write_text("")
    names = zipfile.ZipFile(io.BytesIO(deploy.function_zip(tmp_path))).namelist()
    assert set(names) == {"stream_window.py", "inputs.py", "results.py", "window_function.py", "pydantic/__init__.py"}
    assert deploy.mask("arn:aws:sqs:ca-central-1:111122223333:q", "111122223333").count("123456789012") == 1


# ---- up and down on moto ------------------------------------------------------------------------------------

def test_up_creates_tagged_resources_and_down_leaves_none(aws, tmp_path):
    out = deploy.up(aws, vendor=tmp_path, sleep=lambda s: None, smoke=False)
    assert out["mapping"]["TumblingWindowInSeconds"] == 300 and out["mapping"]["BisectBatchOnFunctionError"] is True
    found = deploy.existing(aws, out["account"])
    made = {k: v for k, v in found.items() if v is not None}
    assert set(made) >= {"table l19-prices", "table l19-windows", "queue l19-failures", "function l19-window",
                         "role recipe-l19-events-lambda", "event source mapping of l19-window"}
    assert deploy.not_ours(found) == []
    policy = aws["iam"].get_role_policy(RoleName=deploy.ROLE, PolicyName="least-privilege")["PolicyDocument"]
    assert len(policy["Statement"]) == 4
    assert deploy.up(aws, vendor=tmp_path, sleep=lambda s: None, smoke=False)["stream_arn"] == out["stream_arn"]   # again: kept
    gone = deploy.down(aws, sleep=lambda s: None)
    assert gone["remaining"] == [] and len(gone["deleted"]) >= 5
    assert all(v is None for v in deploy.existing(aws, out["account"]).values())
    assert deploy.down(aws, sleep=lambda s: None) == {"deleted": [], "remaining": []}


def test_up_and_down_refuse_a_name_that_is_not_ours(aws, tmp_path):
    aws["sqs"].create_queue(QueueName=deploy.QUEUE)                      # someone else's queue with our name
    with pytest.raises(SystemExit, match="not tagged"):
        deploy.up(aws, vendor=tmp_path, sleep=lambda s: None, smoke=False)
    with pytest.raises(SystemExit, match="not deleted"):
        deploy.down(aws, sleep=lambda s: None)
    assert aws["sqs"].get_queue_url(QueueName=deploy.QUEUE)["QueueUrl"]
    assert aws["dynamodb"].list_tables()["TableNames"] == []


def test_a_passed_role_is_not_created_and_not_deleted(aws, tmp_path, monkeypatch):
    trust = json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]})
    arn = aws["iam"].create_role(RoleName="my-own-role", AssumeRolePolicyDocument=trust)["Role"]["Arn"]
    monkeypatch.setenv("LAMBDA_ROLE_ARN", arn)
    out = deploy.up(aws, vendor=tmp_path, sleep=lambda s: None, smoke=False)
    assert out["role_arn"] == arn and deploy.existing(aws, out["account"])[f"role {deploy.ROLE}"] is None
    assert deploy.down(aws, sleep=lambda s: None)["remaining"] == []
    assert aws["iam"].get_role(RoleName="my-own-role")["Role"]["Arn"] == arn


# ---- the clients --------------------------------------------------------------------------------------------

def test_conditional_write_keeps_the_newer_update(aws):
    _tables(aws)
    put = lambda t, uid: clients.put_update(aws["dynamodb"], deploy.PRICES, "p", "north", 2.0, t, uid, if_newer=True)   # noqa: E731
    assert put("2026-10-31T09:00:05Z", "a") is True
    assert put("2026-10-31T09:00:05Z", "b") is False                     # the same time: the first one stays
    assert put("2026-10-31T09:00:01Z", "c") is False                     # older: refused
    assert put("2026-10-31T09:00:09Z", "d") is True


def test_race_has_one_winner_per_round_and_the_newest_item(aws):
    _tables(aws)
    r = clients.run_race(lambda: boto3.client("dynamodb", region_name="ca-central-1"), deploy.PRICES, "t1", rounds=20, stale=5)
    # moto checks the condition and writes in two steps, so two threads can both win a round there. Exactly one
    # winner per round is what the real table gives; the @live scenario and output/aws-live/ check that.
    assert r["writes"] == 50 and sum(r["won"].values()) + sum(r["lost"].values()) == 50
    assert sum(r["won"].values()) >= 20 and sum(r["lost"].values()) >= 10 and r["other_errors"] == []
    assert r["final_item"]["observed_at"] == r["expected_observed_at"]
    assert len(clients.race_lines(r)) == 3


def test_batch_writer_and_reader(aws):
    _tables(aws)
    url = aws["sqs"].create_queue(QueueName=deploy.QUEUE)["QueueUrl"]
    w = clients.write_batch(aws["dynamodb"], deploy.PRICES, "t2", good=6, bad_at=4)
    items = aws["dynamodb"].scan(TableName=deploy.PRICES)["Items"]
    assert len(items) == 7 and sum("price" not in i for i in items) == 1 and {i["store"]["S"] for i in items} == {"batch-t2"}
    # what the function would have stored, and what Lambda would have sent to the queue
    for shard, n in (("shardId-0001", 3), ("shardId-0002", 2)):       # one row per window and shard
        aws["dynamodb"].put_item(TableName=deploy.WINDOWS, Item={
            "pk": {"S": "window#batch-t2"}, "sk": {"S": f"2026-11-18T01:55:00Z#{shard}"}, "count": {"N": str(n)},
            "avg_price": {"N": "4.3"}})
    aws["dynamodb"].put_item(TableName=deploy.WINDOWS, Item={"pk": {"S": "correction#batch-t2"}, "sk": {"S": "e1"}})
    aws["dynamodb"].put_item(TableName=deploy.WINDOWS, Item={"pk": {"S": "window#other"}, "sk": {"S": "w1"},
                                                              "count": {"N": "99"}, "avg_price": {"N": "1"}})
    aws["sqs"].send_message(QueueUrl=url, MessageBody=json.dumps({
        "requestContext": {"condition": "RetryAttemptsExhausted", "approximateInvokeCount": 2},
        "DDBStreamBatchInfo": {"batchSize": 1, "startSequenceNumber": "1", "endSequenceNumber": "1"}}))
    got = clients.wait_for_batch(aws["dynamodb"], aws["sqs"], deploy.WINDOWS, url, w, deadline_s=5, sleep=lambda s: None)
    assert got["observed"] and got["counted"] == 6 and got["corrections"] == 1 and len(got["failures"]) == 1
    lines = clients.batch_lines(w, got)
    assert "6 of 6 good updates counted" in lines[1] and "each once" in lines[1] and "3 + 2" in lines[1]
    assert "batchSize 1" in lines[2] and len(got["windows"]) == 2
    assert clients.read_failures(aws["sqs"], url, wait_s=0) == []        # read once, then deleted


def test_batch_wait_reports_not_observed_at_the_deadline(aws):
    _tables(aws)
    url = aws["sqs"].create_queue(QueueName=deploy.QUEUE)["QueueUrl"]
    w = clients.write_batch(aws["dynamodb"], deploy.PRICES, "t3", good=3, bad_at=2)
    ticks = iter(range(0, 1000, 10))
    got = clients.wait_for_batch(aws["dynamodb"], aws["sqs"], deploy.WINDOWS, url, w, deadline_s=30,
                                 sleep=lambda s: None, clock=lambda: next(ticks))
    assert not got["observed"] and got["counted"] == 0
    assert any("NOT OBSERVED" in line for line in clients.batch_lines(w, got))


def test_load_reports_latency_staleness_and_where_it_ran(aws, monkeypatch):
    _tables(aws)
    monkeypatch.setattr(clients, "where_am_i", lambda: "a test")
    r = clients.run_load(aws["dynamodb"], deploy.PRICES, "t4", users=5, seconds=1.5, writes_per_s=20, consistent=True)
    assert r["reads"] > 10 and r["writes"] > 5 and 0 < r["p50_ms"] <= r["p99_ms"] <= r["max_ms"]
    assert r["stale_answers"] == 0 and r["max_staleness_ms"] == 0        # one process, a consistent read: never stale
    lines = clients.load_lines(r, "ca-central-1")
    assert "strongly consistent" in lines[0] and "a test" in lines[0] and "p99" in lines[1]
    assert len(clients.burst_updates()) == 120
