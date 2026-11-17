"""Determinism and shape tests for the generated events. No AWS, no model."""
import filecmp
import hashlib
import json
import re
from pathlib import Path
from urllib.parse import unquote

import pytest

import make_events as M

ROOT = Path(__file__).parent.parent
EVENTS = ROOT / "events"


def tree(folder: Path) -> dict[str, str]:
    return {p.relative_to(folder).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(folder.rglob("*")) if p.is_file()}


@pytest.fixture(scope="module")
def manifest():
    return json.loads((EVENTS / "manifest.json").read_text())


@pytest.fixture(scope="module")
def cases(manifest):
    return {c["id"]: c for c in manifest["cases"]}


# ---- determinism --------------------------------------------------------------------------------------------

def test_two_runs_give_identical_files_and_match_the_committed_ones(tmp_path):
    committed = tree(EVENTS)
    try:
        M.main(tmp_path / "one")
        M.main(tmp_path / "two")
    finally:
        M.OUT = EVENTS
    one, two = tree(tmp_path / "one"), tree(tmp_path / "two")
    assert one == two
    assert one == committed, "events/ differs from a fresh run: run `uv run python make_events.py` and commit"
    assert not filecmp.dircmp(tmp_path / "one", tmp_path / "two").diff_files


def test_generator_reads_no_clock_and_no_unseeded_randomness():
    src = (ROOT / "make_events.py").read_text()
    for banned in ("datetime.now", "utcnow", "time.time", "uuid4", "uuid.uuid", "random.random(", "random.seed",
                   "os.urandom", "hash("):
        assert banned not in src, banned


# ---- manifest and documents ---------------------------------------------------------------------------------

def test_manifest_counts_and_files(manifest, cases):
    kinds = [c["kind"] for c in manifest["cases"]]
    assert {k: kinds.count(k) for k in set(kinds)} == {"price-updates": 6, "batch-files": 6, "catalog-changes": 8}
    assert len(cases) == len(manifest["cases"])
    for c in cases.values():
        assert re.fullmatch(r"[a-z]+-[a-z]+", c["id"]), c["id"]          # two-word names, no letter codes
        assert c["files"] and all((ROOT / f).is_file() for f in c["files"])
    assert sum(not c["planted"] for c in cases.values()) == 4            # one normal case per kind, two for the catalog


@pytest.mark.parametrize("doc", ["events/README.md", "TRIAGE.md", "STORIES.md"])
def test_documents_name_every_case_and_only_real_cases(doc, cases):
    text = (ROOT / doc).read_text()
    cited = set(re.findall(r"`([a-z]+-[a-z]+)`", text))
    kinds = {"price-updates", "batch-files", "catalog-changes"}
    words = {"curate-prices", "resend-duplicates", "late-events"}          # code words that are not case ids
    assert set(cases) <= cited, sorted(set(cases) - cited)
    assert cited - set(cases) - kinds - words == set(), sorted(cited - set(cases) - kinds - words)


def test_triage_gives_each_case_one_verdict_and_one_lens(cases):
    rows = [r for r in (ROOT / "TRIAGE.md").read_text().splitlines() if re.match(r"\| `[a-z]+-[a-z]+` \|", r)]
    seen = {}
    for r in rows:
        cells = [c.strip() for c in r.strip("|").split("|")]
        cid = cells[0].strip("`")
        assert cells[2] in ("code", "agent"), r
        assert cells[4] in ("every request", "every row or run", "every build", "once at build time"), r
        seen[cid] = cells[2]
    assert set(seen) == set(cases)
    assert sorted(k for k, v in seen.items() if v == "agent") == ["half-file", "new-column"]


def test_stories_map_requirements_to_real_cases(cases):
    text = (ROOT / "STORIES.md").read_text()
    assert len(re.findall(r"^## JS\d\d ", text, re.M)) == 3
    reqs = [r for r in text.splitlines() if re.match(r"\| R\d{3} \|", r)]
    assert len(reqs) >= 12
    for r in reqs:
        assert re.match(r"\| R\d\d0 \|", r), r                           # R010, R020 ...
        tested_by = set(re.findall(r"`([a-z]+-[a-z]+)`", r.split("|")[-3]))
        assert tested_by and tested_by <= set(cases), r
    covered = set(re.findall(r"`([a-z]+-[a-z]+)`", " ".join(reqs)))
    assert set(cases) <= covered, sorted(set(cases) - covered)


# ---- kind a: price updates ----------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def records():
    return [json.loads(line) for line in (EVENTS / "price-updates" / "records.jsonl").read_text().splitlines()]


@pytest.fixture(scope="module")
def truth():
    return json.loads((EVENTS / "price-updates" / "truth.json").read_text())


def test_stream_record_shape(records):
    for r in records:
        assert set(r) == {"eventID", "eventName", "eventVersion", "eventSource", "awsRegion", "dynamodb",
                          "eventSourceARN"}
        d = r["dynamodb"]
        assert r["eventSource"] == "aws:dynamodb" and d["StreamViewType"] == "NEW_AND_OLD_IMAGES"
        assert 21 <= len(d["SequenceNumber"]) <= 40 and d["SequenceNumber"].isdigit()   # StreamRecord: 21 to 40
        assert set(d["Keys"]) == {"product_code", "store"}
        assert set(d["NewImage"]) == {"product_code", "store", "price", "currency", "observed_at", "update_id"}
        assert (r["eventName"] == "MODIFY") == ("OldImage" in d) and r["eventName"] in ("MODIFY", "INSERT")
        assert isinstance(d["ApproximateCreationDateTime"], int) and d["SizeBytes"] > 0
    assert any(r["eventName"] == "INSERT" for r in records)


def test_stream_order_and_redelivery(records, truth):
    seen, again = set(), []
    for r in records:
        again.append(r["eventID"]) if r["eventID"] in seen else seen.add(r["eventID"])
    assert again == truth["planted_event_ids"]["redelivered-update"] and len(again) == 4
    first = {}
    for r in records:
        first.setdefault(r["eventID"], r)
    seqs = [int(r["dynamodb"]["SequenceNumber"]) for r in first.values()]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)           # each record once in the stream, in order
    assert truth["records_in_stream"] == len(seqs) and truth["records_delivered"] == len(records)


def _window(iso_or_epoch):
    if isinstance(iso_or_epoch, int):
        return (iso_or_epoch - int(M.HOUR_START.timestamp())) // M.WINDOW_S
    h, m, s = map(int, iso_or_epoch[11:19].split(":"))
    return ((h - 9) * 3600 + m * 60 + s) // M.WINDOW_S


def test_planted_updates(records, truth):
    by_id = {r["eventID"]: r for r in records}
    planted = truth["planted_event_ids"]
    late = [by_id[i] for i in planted["late-update"]]
    assert len(late) >= 3
    for r in late:
        d = r["dynamodb"]
        assert d["Keys"]["store"]["S"] == "centre"
        assert _window(d["NewImage"]["observed_at"]["S"]) < _window(d["ApproximateCreationDateTime"])
    # every other record is written in the window in which it was observed
    for i, r in by_id.items():
        if i not in planted["late-update"]:
            d = r["dynamodb"]
            assert _window(d["NewImage"]["observed_at"]["S"]) == _window(d["ApproximateCreationDateTime"])
    eur = [r for r in by_id.values() if r["dynamodb"]["NewImage"]["currency"]["S"] != "CAD"]
    assert [r["eventID"] for r in eur] == planted["eur-update"] and len(eur) == 1
    assert eur[0]["dynamodb"]["Keys"]["store"]["S"] == "online"
    ratio = float(eur[0]["dynamodb"]["NewImage"]["price"]["N"]) / float(eur[0]["dynamodb"]["OldImage"]["price"]["N"])
    assert 0.55 < ratio < 0.75                                            # near 1 / 1.55
    burst = [by_id[i] for i in planted["store-burst"]]
    assert len(burst) == 120 and {r["dynamodb"]["Keys"]["store"]["S"] for r in burst} == {"north"}
    assert len({r["dynamodb"]["Keys"]["product_code"]["S"] for r in burst}) == 120


def test_windows(truth):
    w = {x["start"][11:16]: x for x in truth["windows"]}
    assert len(w) == 12
    for start in ("09:40", "09:45"):                                      # the quiet 10 minutes
        for view in ("correct", "as_delivered"):
            assert all(v["count"] == 0 for v in w[start][view].values())
    assert w["09:10"]["as_delivered"]["centre"]["count"] < w["09:10"]["correct"]["centre"]["count"]
    assert w["09:15"]["as_delivered"]["centre"]["count"] > w["09:15"]["correct"]["centre"]["count"]
    assert w["09:20"]["correct"]["north"]["count"] >= 120
    differ = [k for k, x in w.items() if x["correct"] != x["as_delivered"]]
    assert differ == ["09:10", "09:15", "09:25", "09:30"]                 # late, late, redelivered, EUR; no others


def test_window_event_shape():
    e = json.loads((EVENTS / "price-updates" / "window-event.json").read_text())
    assert set(e) == {"Records", "window", "state", "shardId", "eventSourceARN", "isFinalInvokeForWindow",
                      "isWindowTerminatedEarly"}
    assert e["window"] == {"start": "2026-10-31T09:15:00Z", "end": "2026-10-31T09:20:00Z"} and e["Records"]


# ---- kind b: batch files ------------------------------------------------------------------------------------

def _batch(case):
    folder = EVENTS / "batch-files" / case
    events = json.loads((folder / "s3-events.json").read_text())
    return folder, events


@pytest.mark.parametrize("case", list(M.BATCH_CASES))
def test_s3_events_match_their_files(case, cases):
    folder, events = _batch(case)
    assert len(events) == cases[case]["facts"]["events"] == (2 if case == "missing-store" else 3)
    times = []
    for e in events:
        (rec,) = e["Records"]
        assert rec["eventSource"] == "aws:s3" and rec["eventName"] == "ObjectCreated:Put"
        assert rec["eventVersion"].split(".")[0] == "2"
        obj = rec["s3"]["object"]
        assert "%3D" in obj["key"] and "=" not in obj["key"]             # the key is URL encoded
        data = (folder / unquote(obj["key"])).read_bytes()
        assert obj["size"] == len(data) and obj["eTag"] == hashlib.md5(data).hexdigest()
        assert re.fullmatch(r"[0-9A-F]{20}", obj["sequencer"])
        times.append(rec["eventTime"])
    assert times == sorted(times)


def test_batch_cases_show_what_they_plant(cases):
    base = json.loads((EVENTS / "batch-files" / "baseline.json").read_text())["per_store"]
    f = {c: cases[c]["facts"]["per_store"] for c in M.BATCH_CASES}
    normal = f["normal-day"]
    assert sum(x["rows"] for x in normal.values()) == 1480               # L18's job log: read 1480 rows from 3 files
    assert sum(x["rows"] - x["products"] for x in normal.values()) == 247   # ... removed 247 duplicate observations
    for store, x in normal.items():
        usual = base[store]["usual"]
        assert x["products"] == M.PRODUCTS and x["repeated_obs_ids"] == 0 and not x["arrived_after_job_start"]
        assert abs(x["rows"] / usual["rows"] - 1) <= 0.05
        assert abs(x["avg_price_cad"] / usual["avg_price_cad"] - 1) < 0.08
        assert len(base[store]["days"]) == 7
    assert normal["online"]["currencies"] == ["EUR"] and normal["north"]["currencies"] == ["CAD"]
    assert f["duplicate-rows"]["centre"]["repeated_obs_ids"] == normal["centre"]["rows"]
    assert "online" not in f["missing-store"]
    nc = f["not-converted"]["online"]
    assert nc["currencies"] == ["CAD"] and nc["avg_price"] == normal["online"]["avg_price"]
    ratio = nc["avg_price_cad"] / base["online"]["usual"]["avg_price_cad"]           # what the handler compares
    assert abs(ratio - 1) > 0.08 and abs(ratio - 1 / M.EUR_TO_CAD) < 0.05              # triage/agent.py verify()
    rows_off = lambda c, s: abs(f[c][s]["rows"] / base[s]["usual"]["rows"] - 1) > 0.05   # noqa: E731
    assert rows_off("half-file", "centre") and rows_off("duplicate-rows", "centre")
    assert not rows_off("not-converted", "online") and not rows_off("late-file", "centre")
    half = f["half-file"]["centre"]
    assert half["products"] == M.PRODUCTS // 2 and half["repeated_obs_ids"] == 0
    assert abs(half["avg_price_cad"] / base["centre"]["usual"]["avg_price_cad"] - 1) < 0.08   # prices do not give it away
    late = f["late-file"]
    assert late["centre"]["arrived_after_job_start"] and not late["north"]["arrived_after_job_start"]
    for case in ("duplicate-rows", "not-converted", "half-file", "late-file"):       # the other stores are untouched
        assert f[case]["north"] == normal["north"]


# ---- kind c: catalog changes --------------------------------------------------------------------------------

@pytest.mark.parametrize("case", ["new-partition", "new-table", "new-column", "type-change", "renamed-column",
                                  "dropped-column", "repeated-event", "no-event"])
def test_glue_event_shape(case, cases):
    folder = EVENTS / "catalog-changes" / case
    events = json.loads((folder / "events.json").read_text())
    assert len(events) == cases[case]["facts"]["events"]
    for e in events:
        assert set(e) == {"version", "id", "detail-type", "source", "account", "time", "region", "resources", "detail"}
        assert e["source"] == "aws.glue" and e["version"] == "0"
        d = e["detail"]
        if e["detail-type"] == "Glue Data Catalog Table State Change":
            assert d["typeOfChange"] in ("UpdateTable", "CreatePartition", "BatchCreatePartition", "UpdatePartition",
                                         "DeletePartition", "BatchUpdatePartition", "BatchDeletePartition")
            assert set(d) - {"changedPartitions"} == {"databaseName", "tableName", "typeOfChange"}
            assert not any(k.lower().startswith("column") for k in d)     # the event does not carry columns
        else:
            assert e["detail-type"] == "Glue Data Catalog Database State Change"
            assert d["typeOfChange"] in ("CreateDatabase", "DeleteDatabase", "CreateTable", "DeleteTable",
                                         "BatchDeleteTable")
            assert set(d) == {"databaseName", "typeOfChange", "changedTables"}
        assert d["databaseName"] == M.DATABASE
    after = json.loads((folder / "after.json").read_text())["Table"]
    assert {"Name", "VersionId", "StorageDescriptor", "PartitionKeys"} <= set(after)


def test_job_read_list_separates_the_column_cases(manifest, cases):
    reads = set(manifest["job"]["reads"])
    f = {c["id"]: c["facts"] for c in cases.values() if c["kind"] == "catalog-changes"}
    hit = lambda c: bool(reads & set(f[c]["removed"] + f[c]["retyped"]))   # noqa: E731
    assert hit("type-change") and hit("renamed-column") and hit("no-event") and hit("repeated-event")
    assert not hit("dropped-column") and not hit("new-column") and not hit("new-partition")


def test_catalog_cases_show_what_they_plant(cases):
    f = {c["id"]: c["facts"] for c in cases.values() if c["kind"] == "catalog-changes"}
    assert f["new-partition"] == {"events": 1, "added": [], "removed": [], "retyped": []}
    assert f["new-column"]["added"] == ["on_promotion"] and f["type-change"]["retyped"] == ["price"]
    assert (f["renamed-column"]["removed"], f["renamed-column"]["added"]) == (["price"], ["unit_price"])   # as in L18
    assert f["dropped-column"]["removed"] == ["source"]
    assert f["no-event"]["events"] == 0 and f["no-event"]["removed"] == ["currency"]
    ev = json.loads((EVENTS / "catalog-changes" / "repeated-event" / "events.json").read_text())
    times = [e["time"] for e in ev]
    assert times != sorted(times) and ev[0] == ev[2] and ev[0]["id"] != ev[1]["id"]
    # the four column cases cannot be told apart from the event alone
    details = [json.loads((EVENTS / "catalog-changes" / c / "events.json").read_text())[0]["detail"]
               for c in ("new-column", "type-change", "renamed-column", "dropped-column")]
    assert all(d == details[0] for d in details)
    part = json.loads((EVENTS / "catalog-changes" / "new-partition" / "events.json").read_text())[0]["detail"]
    assert part["changedPartitions"] == ["2026-11"] and part["tableName"] == "prices_curated"
