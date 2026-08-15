"""Gold dimension tables loaded from data/raw/reference/*.json.

carriers.json and organisations.json aren't raw tracking streams. so they're
loaded straight into gold rather than run through bronze/silver ceremony
that doesn't apply to them. If a row fails to parse here it's raised, not
quarantined.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, ListType, NestedField, StringType, TimestampType

from src.common.catalog import get_catalog, get_or_create_table, overwrite_table

REFERENCE_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" / "reference"

CARRIERS_TABLE_IDENTIFIER = "gold.dim_carriers"
ORGANISATIONS_TABLE_IDENTIFIER = "gold.dim_organisations"

CARRIERS_SCHEMA = Schema(
    NestedField(1, "carrier_code", StringType(), required=True),
    NestedField(2, "carrier_name", StringType(), required=True),
    NestedField(3, "sla_hours", IntegerType(), required=True),
    NestedField(
        4, "countries_served", ListType(element_id=5, element=StringType(), element_required=False), required=False
    ),
)

ORGANISATIONS_SCHEMA = Schema(
    NestedField(1, "organisation_id", IntegerType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "country", StringType(), required=False),
    NestedField(4, "plan", StringType(), required=False),
    NestedField(5, "created_at", TimestampType(), required=False),
)


@dataclass
class TransformResult:
    rows_loaded: int


def _read_jsonl(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _parse_iso8601_utc(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def build_carriers(catalog: Catalog, reference_dir: Path = REFERENCE_DIR) -> TransformResult:
    table = get_or_create_table(catalog, CARRIERS_TABLE_IDENTIFIER, CARRIERS_SCHEMA, PartitionSpec())
    rows = [
        {
            "carrier_code": r["carrier_code"],
            "carrier_name": r["carrier_name"],
            "sla_hours": r["sla_hours"],
            "countries_served": r.get("countries_served"),
        }
        for r in _read_jsonl(reference_dir / "carriers.json")
    ]
    overwrite_table(table, rows)
    return TransformResult(rows_loaded=len(rows))


def build_organisations(catalog: Catalog, reference_dir: Path = REFERENCE_DIR) -> TransformResult:
    table = get_or_create_table(catalog, ORGANISATIONS_TABLE_IDENTIFIER, ORGANISATIONS_SCHEMA, PartitionSpec())
    rows = [
        {
            "organisation_id": r["organisation_id"],
            "name": r.get("name"),
            "country": r.get("country"),
            "plan": r.get("plan"),
            "created_at": _parse_iso8601_utc(r.get("created_at")),
        }
        for r in _read_jsonl(reference_dir / "organisations.json")
    ]
    overwrite_table(table, rows)
    return TransformResult(rows_loaded=len(rows))


def run() -> TransformResult:
    catalog = get_catalog()
    carriers = build_carriers(catalog)
    organisations = build_organisations(catalog)
    return TransformResult(rows_loaded=carriers.rows_loaded + organisations.rows_loaded)
