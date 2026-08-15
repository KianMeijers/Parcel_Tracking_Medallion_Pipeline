"""Silver-layer cleaning of the internal parcel_events lifecycle stream.

This module's job is dedup, type-casting, and quarantining semantically
bad rows, leaving the milestone-level modeling (transit time, SLA
on-time-ness) to gold.

Grain: one row per event_id .

recipient_email is deliberately dropped here rather than carried forward.
None of the target business questions (parcel counts, on-time rates,
delivery-performance trends) need it, and it's the one column in this
dataset that's personal information 

Idempotency & completeness: same full-table-overwrite approach as
silver/tracking_events.py - see that module's docstring for the reasoning.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timezone

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import DayTransform, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType, TimestampType

from src.common.catalog import get_catalog, get_or_create_table, overwrite_table

TABLE_IDENTIFIER = "silver.parcel_events"
QUARANTINE_TABLE_IDENTIFIER = "silver.parcel_events_quarantine"
BRONZE_TABLE_IDENTIFIER = "bronze.parcel_events"

REQUIRED_FIELDS = (
    "event_id",
    "event_type",
    "parcel_id",
    "tracking_number",
    "organisation_id",
    "carrier_code",
)

RECORD_SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "event_type", StringType(), required=True),
    NestedField(3, "parcel_id", IntegerType(), required=True),
    NestedField(4, "tracking_number", StringType(), required=True),
    NestedField(5, "organisation_id", IntegerType(), required=True),
    NestedField(6, "carrier_code", StringType(), required=True),
    NestedField(7, "service_level", StringType(), required=False),
    NestedField(8, "destination_country", StringType(), required=False),
    NestedField(9, "destination_postcode", StringType(), required=False),
    NestedField(10, "occurred_at", TimestampType(), required=True),
    NestedField(11, "source_file", StringType(), required=True),
    NestedField(12, "source_line_no", IntegerType(), required=True),
    NestedField(13, "_silver_loaded_at", TimestampType(), required=True),
)
RECORD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=10, field_id=1000, transform=DayTransform(), name="occurred_day")
)

QUARANTINE_SCHEMA = Schema(
    NestedField(1, "source_file", StringType(), required=True),
    NestedField(2, "source_line_no", IntegerType(), required=True),
    NestedField(3, "reason", StringType(), required=True),
    NestedField(4, "raw_record", StringType(), required=True),
    NestedField(5, "_silver_loaded_at", TimestampType(), required=True),
)
QUARANTINE_PARTITION_SPEC = PartitionSpec()


@dataclass
class TransformResult:
    rows_loaded: int
    rows_quarantined: int


def _parse_iso8601_utc(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _normalize(row: dict) -> tuple[dict | None, str | None]:
    # `is None`, not a truthiness check - parcel_id/organisation_id are ints
    # and a legitimate 0 must not be treated as missing.
    missing = [field for field in REQUIRED_FIELDS if row.get(field) is None]
    if missing:
        return None, f"missing_fields:{','.join(missing)}"
    occurred_at = _parse_iso8601_utc(row.get("occurred_at"))
    if occurred_at is None:
        return None, f"unparseable_occurred_at:{row.get('occurred_at')}"
    return {
        "event_id": row["event_id"],
        "event_type": row["event_type"],
        "parcel_id": row["parcel_id"],
        "tracking_number": row["tracking_number"],
        "organisation_id": row["organisation_id"],
        "carrier_code": row["carrier_code"],
        "service_level": row.get("service_level"),
        "destination_country": row.get("destination_country"),
        "destination_postcode": row.get("destination_postcode"),
        "occurred_at": occurred_at,
    }, None


def build(catalog: Catalog) -> TransformResult:
    table = get_or_create_table(catalog, TABLE_IDENTIFIER, RECORD_SCHEMA, RECORD_PARTITION_SPEC)
    quarantine_table = get_or_create_table(
        catalog, QUARANTINE_TABLE_IDENTIFIER, QUARANTINE_SCHEMA, QUARANTINE_PARTITION_SPEC
    )

    loaded_at = datetime.now(timezone.utc)
    good_by_key: dict[str, dict] = {}
    bad_rows: list[dict] = []

    if catalog.table_exists(BRONZE_TABLE_IDENTIFIER):
        bronze_rows = catalog.load_table(BRONZE_TABLE_IDENTIFIER).scan().to_arrow().to_pylist()
        bronze_rows.sort(key=lambda r: (r["source_file"], r["source_line_no"]))

        for row in bronze_rows:
            normalized, error = _normalize(row)
            if error is not None:
                bad_rows.append(
                    {
                        "source_file": row["source_file"],
                        "source_line_no": row["source_line_no"],
                        "reason": error,
                        "raw_record": json.dumps(row, default=str),
                        "_silver_loaded_at": loaded_at,
                    }
                )
                continue
            event_id = normalized["event_id"]
            if event_id in good_by_key:
                continue  # retried/duplicated internal event
            normalized["source_file"] = row["source_file"]
            normalized["source_line_no"] = row["source_line_no"]
            normalized["_silver_loaded_at"] = loaded_at
            good_by_key[event_id] = normalized

    overwrite_table(table, list(good_by_key.values()))
    overwrite_table(quarantine_table, bad_rows)

    return TransformResult(rows_loaded=len(good_by_key), rows_quarantined=len(bad_rows))


def run() -> TransformResult:
    return build(get_catalog())
