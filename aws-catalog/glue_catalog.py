"""The catalog the drafter's function reads on AWS: the same interface as local.LocalCatalog, so catalog_watch.py
and column_drafter.py run unchanged in Lambda.

    get_table       Glue GetTable (the table as it is now: VersionId, columns with name, type and comment)
    tables          Glue GetTables for the database, and every table a definition was stored for
    stored          the definitions the watcher saw last, one S3 object per table: state/<table>.json
    samples         sample values of a column, read from samples/<table>.<column>.json. In this demo `catalog-run`
                    puts them there; a real system would read them with a query on the table's data
    job_reads       the columns the nightly job reads, from reference/job.json
"""
from __future__ import annotations

import json

from botocore.exceptions import ClientError


class StoredDefinitions:
    """A dict of table name -> definition, kept as S3 objects under state/."""

    def __init__(self, s3, bucket: str):
        self.s3, self.bucket = s3, bucket

    def get(self, name: str, default=None):
        try:
            return json.loads(self.s3.get_object(Bucket=self.bucket, Key=f"state/{name}.json")["Body"].read())
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchKey":
                raise
            return default

    def __getitem__(self, name: str):
        found = self.get(name)
        if found is None:
            raise KeyError(name)
        return found

    def __setitem__(self, name: str, definition: dict) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=f"state/{name}.json",
                           Body=json.dumps(definition, default=str).encode(), ContentType="application/json")

    def __iter__(self):
        page = self.s3.list_objects_v2(Bucket=self.bucket, Prefix="state/")
        return iter(o["Key"][len("state/"):-len(".json")] for o in page.get("Contents", []))


class GlueCatalog:
    def __init__(self, glue, s3, bucket: str, database: str):
        self.glue, self.s3, self.bucket, self.database = glue, s3, bucket, database
        self.stored = StoredDefinitions(s3, bucket)
        self.job_reads = json.loads(s3.get_object(Bucket=bucket, Key="reference/job.json")["Body"].read())["reads"]

    def get_table(self, database: str, table: str) -> dict | None:
        """The table as Glue has it now, cut to the fields the watcher compares (the shape of local before.json)."""
        try:
            t = self.glue.get_table(DatabaseName=self.database, Name=table)["Table"]
        except self.glue.exceptions.EntityNotFoundException:
            return None
        cols = lambda xs: [{"Name": c["Name"], "Type": c["Type"], "Comment": c.get("Comment", "")} for c in xs]   # noqa: E731
        return {"Table": {"Name": t["Name"], "DatabaseName": self.database, "VersionId": t.get("VersionId", "0"),
                          "UpdateTime": str(t.get("UpdateTime", "")),
                          "StorageDescriptor": {"Columns": cols(t.get("StorageDescriptor", {}).get("Columns", []))},
                          "PartitionKeys": cols(t.get("PartitionKeys", []))}}

    def tables(self) -> list[str]:
        names = {t["Name"] for t in self.glue.get_tables(DatabaseName=self.database)["TableList"]}
        return sorted(names | set(self.stored))

    def samples(self, table: str, column: str) -> list:
        try:
            body = self.s3.get_object(Bucket=self.bucket, Key=f"samples/{table}.{column}.json")["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchKey":
                raise
            return []
        return json.loads(body)["values"]
