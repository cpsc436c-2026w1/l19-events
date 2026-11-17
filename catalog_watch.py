"""A table in the Glue Data Catalog changed: compare the stored definition with the current one.

A Lambda function on the EventBridge events "Glue Data Catalog Table State Change" and "Glue Data Catalog
Database State Change". The event names the table and the operation, not the columns, and AWS sends it best
effort (out of order, twice, or not at all). So the handler never acts on the event's content: it reads the
table as it is now and compares it with the definition it stored, by VersionId. `daily_handler` makes the same
comparison for every table on a schedule, for a change that sent no event.

    actions   none | record | block_job | alert_owners | draft_description

    uv run python catalog_watch.py                # every catalog case
"""
from __future__ import annotations

import sys

from local import LocalCatalog, manifest
from results import CatalogResult

CATALOG: LocalCatalog | None = None                # set by the caller; a Glue client and a state table in AWS


def _columns(table: dict | None) -> dict[str, dict]:
    return {c["Name"]: c for c in table["Table"]["StorageDescriptor"]["Columns"]} if table else {}


def compare(name: str, stored: dict | None, current: dict | None, job_reads: list[str]) -> CatalogResult:
    sv = stored["Table"]["VersionId"] if stored else None
    cv = current["Table"]["VersionId"] if current else None
    base = dict(table=name, stored_version=sv, current_version=cv)
    if current is None:
        return CatalogResult(**base, actions=["none"], reasons=["the table is not in the catalog"])
    cols = _columns(current)
    lacking = [n for n, c in cols.items() if not c.get("Comment")]
    if stored is None:                                               # a table the watcher has not seen
        return CatalogResult(**base, actions=["record"], lacking_description=lacking, reasons=["new table"])
    if int(cv) <= int(sv):                                           # the same or an older event: nothing new
        return CatalogResult(**base, actions=["none"], reasons=["the stored definition is current"])
    old = _columns(stored)
    added = [n for n in cols if n not in old]
    removed = [n for n in old if n not in cols]
    retyped = [n for n in cols if n in old and cols[n]["Type"] != old[n]["Type"]]
    actions, reasons = ["record"], []
    broken = [n for n in removed + retyped if n in job_reads]
    if broken:
        actions.append("block_job")
        reasons.append(f"the job reads {broken}, now missing or of another type")
    if [n for n in removed if n not in job_reads]:
        actions.append("alert_owners")
        reasons.append(f"removed and not read by the job: {[n for n in removed if n not in job_reads]}")
    undescribed = [n for n in added if n in lacking]
    if undescribed:
        actions.append("draft_description")
        reasons.append(f"new columns without a description: {undescribed}")
    return CatalogResult(**base, actions=actions, alert="block_job" in actions or "alert_owners" in actions,
                         added=added, removed=removed, retyped=retyped,
                         lacking_description=undescribed, reasons=reasons)


def _watch(name: str, database: str = "") -> CatalogResult:
    current = CATALOG.get_table(database, name)
    result = compare(name, CATALOG.stored.get(name), current, CATALOG.job_reads)
    if "record" in result.actions:
        CATALOG.stored[name] = current                               # the next event compares with this one
    return result


def handler(event: dict, context=None) -> list[dict]:
    d = event["detail"]
    if d["typeOfChange"].endswith("Partition"):                      # partitions do not change the columns
        return [CatalogResult(table=d["tableName"], stored_version=None, current_version=None, actions=["record"],
                              reasons=[f"partitions {d.get('changedPartitions', [])}"]).model_dump()]
    names = d.get("changedTables") or [d["tableName"]]
    return [_watch(n, d["databaseName"]).model_dump() for n in names]


def daily_handler(event: dict, context=None) -> list[dict]:
    return [_watch(n).model_dump() for n in CATALOG.tables()]


if __name__ == "__main__":
    wanted = sys.argv[1:] or [c["id"] for c in manifest()["cases"] if c["kind"] == "catalog-changes"]
    for case in wanted:
        CATALOG = LocalCatalog(case)
        results = [r for e in CATALOG.events() for r in handler(e)] or daily_handler({})
        for r in results:
            print(f"{case:15} {r['table']:20} {','.join(r['actions']):32} alert={r['alert']!s:5} {'; '.join(r['reasons'])}")
