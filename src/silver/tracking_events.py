"""Silver-layer consolidation of carrier_a/carrier_b/carrier_c tracking
events into one unified table.

Grain: one row per (carrier_code, tracking_number, status, event_time) -
matching how each bronze module already describes its own retry duplicates
("repeated (tracking_no, status, ts) triples are expected raw duplicates").
Collapsing on that exact triple is what dedupes webhook retries.

Status vocabulary
------------------
carrier_a and carrier_b already agree on their status sets:

    ACCEPTED, AT_HUB, IN_TRANSIT, OUT_FOR_DELIVERY, DELIVERED,
    DELIVERY_FAILED, RETURNED, LOST

carrier_c's `st` is an undocumented int.
The mapping below is inferred from the data itself, not from any carrier
doc, and is the one material judgment call in this module:

Timestamps
----------
carrier_a and the internal parcel_events stream use ISO 8601 with an
explicit "Z". carrier_c's `t` is epoch milliseconds (unambiguous). Only
carrier_b's `event_time` is ambiguous - a naive "YYYY-MM-DD HH:MM:SS" with
no offset. There's no reliable signal to resolve it either way: comparing
carrier_b's hour-of-day distribution against carrier_a's (confirmed UTC)
shows both roughly flat/uniform with no shift, so there's no business-hours
pattern to align on. Given no evidence of a non-UTC offset, it's treated as
UTC - the same assumption the bronze module flagged as deferred to silver.
If real carrier docs later specify a timezone,
`_parse_carrier_b_event_time` is the  place to fix.

Bad data
--------
A structurally-valid JSON row can still be semantically bad: a missing
identifier or an unparseable/unrecognized status or timestamp. Those are
quarantined here into `silver.tracking_events_quarantine`, one row per
rejected bronze record with a `reason` and the original record for
debugging.

Idempotency & completeness
---------------------------
Each run reads all rows currently in the three bronze tables and fully
overwrites both silver tables (no per-source-file scoping like bronze
uses). Done to have "state completeness" - a case finished by a file
that lands tomorrow - come out correct automatically: tomorrow's run just
recomputes over the bigger bronze tables. It also makes reruns
idempotent without the complexer merge/upsert logic.
This is a pragmatic choice for the dataset's current scale.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timezone

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import DayTransform, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, IntegerType, NestedField, StringType, TimestampType

from src.common.catalog import get_catalog, get_or_create_table, overwrite_table

TABLE_IDENTIFIER = "silver.tracking_events"
QUARANTINE_TABLE_IDENTIFIER = "silver.tracking_events_quarantine"

STATUSES = frozenset(
    {
        "ACCEPTED",
        "AT_HUB",
        "IN_TRANSIT",
        "OUT_FOR_DELIVERY",
        "DELIVERED",
        "DELIVERY_FAILED",
        "RETURNED",
        "LOST",
    }
)

RECORD_SCHEMA = Schema(
    NestedField(1, "tracking_number", StringType(), required=True),
    NestedField(2, "carrier_code", StringType(), required=True),
    NestedField(3, "status", StringType(), required=True),
    NestedField(4, "event_time", TimestampType(), required=True),
    NestedField(5, "weight_kg", DoubleType(), required=False),
    NestedField(6, "facility_code", StringType(), required=False),
    NestedField(7, "raw_status", StringType(), required=True),
    NestedField(8, "source_file", StringType(), required=True),
    NestedField(9, "source_line_no", IntegerType(), required=True),
    NestedField(10, "_silver_loaded_at", TimestampType(), required=True),
)
RECORD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=DayTransform(), name="event_day")
)

QUARANTINE_SCHEMA = Schema(
    NestedField(1, "carrier_code", StringType(), required=True),
    NestedField(2, "source_file", StringType(), required=True),
    NestedField(3, "source_line_no", IntegerType(), required=True),
    NestedField(4, "reason", StringType(), required=True),
    NestedField(5, "raw_record", StringType(), required=True),
    NestedField(6, "_silver_loaded_at", TimestampType(), required=True),
)
QUARANTINE_PARTITION_SPEC = PartitionSpec()

CARRIER_B_STATUS_BY_CODE = {
    10: "ACCEPTED",
    20: "IN_TRANSIT",
    30: "AT_HUB",
    40: "OUT_FOR_DELIVERY",
    45: "DELIVERED",
    50: "DELIVERY_FAILED",
    60: "RETURNED",
    99: "LOST",
}

CARRIER_C_STATUS_BY_CODE = {
    0: "ACCEPTED",
    1: "IN_TRANSIT",
    2: "OUT_FOR_DELIVERY",
    3: "DELIVERED",
    4: "DELIVERY_FAILED",
    5: "RETURNED",
    9: "LOST",
}


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


def _parse_carrier_b_event_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_epoch_millis_utc(value: object) -> datetime | None:
    if not isinstance(value, (int, float)):
        return None
    try:
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _carrier_c_weight_kg(dims_json: object) -> float | None:
    if not isinstance(dims_json, str):
        return None
    try:
        weight = json.loads(dims_json).get("weight")
    except (json.JSONDecodeError, AttributeError):
        return None
    if isinstance(weight, (int, float)):
        return float(weight)
    if isinstance(weight, dict) and weight.get("u") == "kg" and isinstance(weight.get("v"), (int, float)):
        return float(weight["v"])
    return None


def _normalize_carrier_a(row: dict) -> tuple[dict | None, str | None]:
    tracking_number = row.get("tracking_no")
    if not tracking_number:
        return None, "missing_tracking_number"
    status = row.get("status")
    if status not in STATUSES:
        return None, f"unrecognized_status:{status}"
    event_time = _parse_iso8601_utc(row.get("ts"))
    if event_time is None:
        return None, f"unparseable_event_time:{row.get('ts')}"
    return {
        "tracking_number": tracking_number,
        "carrier_code": "carrier_a",
        "status": status,
        "event_time": event_time,
        "weight_kg": row.get("weight_kg"),
        "facility_code": row.get("hub"),
        "raw_status": status,
    }, None


def _normalize_carrier_b(row: dict) -> tuple[dict | None, str | None]:
    tracking_number = row.get("barcode")
    if not tracking_number:
        return None, "missing_tracking_number"
    event = row.get("event") or {}
    code = event.get("code")
    status = CARRIER_B_STATUS_BY_CODE.get(code)
    if status is None:
        return None, f"unrecognized_status_code:{code}"
    event_time = _parse_carrier_b_event_time(row.get("event_time"))
    if event_time is None:
        return None, f"unparseable_event_time:{row.get('event_time')}"
    weight_g = row.get("weight_g")
    weight_kg = weight_g / 1000 if isinstance(weight_g, (int, float)) else None
    return {
        "tracking_number": tracking_number,
        "carrier_code": "carrier_b",
        "status": status,
        "event_time": event_time,
        "weight_kg": weight_kg,
        "facility_code": row.get("depot"),
        "raw_status": f"{code}:{event.get('desc')}",
    }, None


def _normalize_carrier_c(row: dict) -> tuple[dict | None, str | None]:
    tracking_number = row.get("ref")
    if not tracking_number:
        return None, "missing_tracking_number"
    st = row.get("st")
    status = CARRIER_C_STATUS_BY_CODE.get(st)
    if status is None:
        return None, f"unrecognized_status_code:{st}"
    event_time = _parse_epoch_millis_utc(row.get("t"))
    if event_time is None:
        return None, f"unparseable_event_time:{row.get('t')}"
    return {
        "tracking_number": tracking_number,
        "carrier_code": "carrier_c",
        "status": status,
        "event_time": event_time,
        "weight_kg": _carrier_c_weight_kg(row.get("dims")),
        "facility_code": None,
        "raw_status": str(st),
    }, None


SOURCES = (
    ("carrier_a", "bronze.carrier_a", _normalize_carrier_a),
    ("carrier_b", "bronze.carrier_b", _normalize_carrier_b),
    ("carrier_c", "bronze.carrier_c", _normalize_carrier_c),
)


def build(catalog: Catalog) -> TransformResult:
    table = get_or_create_table(catalog, TABLE_IDENTIFIER, RECORD_SCHEMA, RECORD_PARTITION_SPEC)
    quarantine_table = get_or_create_table(
        catalog, QUARANTINE_TABLE_IDENTIFIER, QUARANTINE_SCHEMA, QUARANTINE_PARTITION_SPEC
    )

    loaded_at = datetime.now(timezone.utc)
    good_by_key: dict[tuple, dict] = {}
    bad_rows: list[dict] = []

    for carrier_code, bronze_identifier, normalize in SOURCES:
        if not catalog.table_exists(bronze_identifier):
            continue
        bronze_rows = catalog.load_table(bronze_identifier).scan().to_arrow().to_pylist()
        # Sort so that which physical duplicate "wins" a retry is
        # deterministic across reruns, keeping the output idempotent 
        bronze_rows.sort(key=lambda r: (r["source_file"], r["source_line_no"]))

        for row in bronze_rows:
            normalized, error = normalize(row)
            if error is not None:
                bad_rows.append(
                    {
                        "carrier_code": carrier_code,
                        "source_file": row["source_file"],
                        "source_line_no": row["source_line_no"],
                        "reason": error,
                        "raw_record": json.dumps(row, default=str),
                        "_silver_loaded_at": loaded_at,
                    }
                )
                continue
            key = (normalized["carrier_code"], normalized["tracking_number"], normalized["status"], normalized["event_time"])
            if key in good_by_key:
                continue  # webhook retry of an already-seen event
            normalized["source_file"] = row["source_file"]
            normalized["source_line_no"] = row["source_line_no"]
            normalized["_silver_loaded_at"] = loaded_at
            good_by_key[key] = normalized

    overwrite_table(table, list(good_by_key.values()))
    overwrite_table(quarantine_table, bad_rows)

    return TransformResult(rows_loaded=len(good_by_key), rows_quarantined=len(bad_rows))


def run() -> TransformResult:
    return build(get_catalog())
