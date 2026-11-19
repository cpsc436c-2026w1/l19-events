"""The data that enters a handler, as Pydantic models: a price update, a row of an export file, a catalog event.

A handler parses its input with one of these before it does anything else. Input that does not fit fails here,
at the boundary, with a message that names the field, and not three steps later as a KeyError or a wrong average.

    uv run python inputs.py            # the three injected invalid price updates, and what each message says
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

Store = Literal["north", "centre", "online"]


class PriceUpdate(BaseModel):
    """One change record of the latest-price table, cut to what the window function reads."""
    event_id: str
    event_name: Literal["INSERT", "MODIFY", "REMOVE"]
    sequence_number: str = Field(min_length=21, max_length=40)        # DynamoDB StreamRecord: 21 to 40 characters
    created_at: int                                                   # ApproximateCreationDateTime, epoch seconds
    product_code: str = Field(min_length=1)
    store: Store
    price: float = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)                 # any currency parses; the handler rejects non-CAD
    observed_at: dt.datetime                                          # when the store changed the price
    update_id: str

    @classmethod
    def from_record(cls, record: dict) -> "PriceUpdate":
        """From the DynamoDB Streams record shape ({"S": ...}, {"N": ...}) to plain fields."""
        body = record.get("dynamodb", {})
        image = {k: next(iter(v.values())) for k, v in body.get("NewImage", {}).items() if isinstance(v, dict) and v}
        return cls.model_validate({"event_id": record.get("eventID"), "event_name": record.get("eventName"),
                                   "sequence_number": body.get("SequenceNumber"),
                                   "created_at": body.get("ApproximateCreationDateTime"), **image})


class BatchRow(BaseModel):
    """One row of a store's export file in prices_raw."""
    model_config = ConfigDict(extra="forbid")
    obs_id: str = Field(min_length=32, max_length=32)
    product_code: str = Field(min_length=1)
    store: Store
    observed_on: dt.date
    price: float = Field(gt=0)
    currency: Literal["CAD", "EUR"]
    source: str


class TableChange(BaseModel):
    databaseName: str
    tableName: str
    typeOfChange: Literal["UpdateTable", "CreatePartition", "BatchCreatePartition", "UpdatePartition",
                          "DeletePartition", "BatchUpdatePartition", "BatchDeletePartition"]
    changedPartitions: list[str] = []


class DatabaseChange(BaseModel):
    databaseName: str
    typeOfChange: Literal["CreateDatabase", "DeleteDatabase", "CreateTable", "DeleteTable", "BatchDeleteTable"]
    changedTables: list[str] = []


class CatalogEvent(BaseModel):
    """One EventBridge event of the Glue Data Catalog (Glue developer guide, automating-awsglue-with-cloudwatch-events)."""
    id: str
    source: Literal["aws.glue"]
    detail_type: Literal["Glue Data Catalog Table State Change", "Glue Data Catalog Database State Change"] = Field(
        alias="detail-type")
    time: dt.datetime
    detail: TableChange | DatabaseChange


def problems(error: ValidationError) -> list[str]:
    """One line per invalid field: its name, what is wrong, and the value received."""
    out = []
    for e in error.errors():
        field = ".".join(str(p) for p in e["loc"] if p not in ("TableChange", "DatabaseChange")) or "(input)"
        got = "" if e["type"] == "missing" else f" (got {e['input']!r})"[:60]
        out.append(f"{field}: {e['msg']}{got}")
    return out


if __name__ == "__main__":
    path = Path(__file__).parent / "events" / "price-updates" / "invalid-records.jsonl"
    for line in path.read_text().splitlines():
        record = json.loads(line)
        try:
            PriceUpdate.from_record(record)
        except ValidationError as e:
            print(f"{record['dynamodb']['NewImage']['update_id']['S']}  rejected  {'; '.join(problems(e))}")
