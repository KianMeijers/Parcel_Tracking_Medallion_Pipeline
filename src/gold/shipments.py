"""Gold fact table: one row per parcel, joining the internal lifecycle
(silver.parcel_events) with the carrier's own scan history
(silver.tracking_events) and each carrier's SLA (gold.dim_carriers).

"Shipped" = parcel_handed_over (parcel_events - the org's own dispatch
record), which is not the same moment as "accepted" = the carrier's first
ACCEPTED scan (tracking_events). 
!This has to be confirmed with the business.!

3,603 of 500,000 real parcels have a parcel_events record but never appear
in tracking_events (handed over, not yet/never scanned). Those rows keep
accepted_at/delivered_at/transit_hours/is_on_time as null rather than being
dropped - still real shipments for a "how many did org X ship" count, just
unresolved for "was it on time".

Checked against the real silver tables: no parcel has
more than one ACCEPTED or DELIVERED event, weight is identical across every
event for a given parcel, and every parcel_events attribute (org, carrier,
destination) is identical across a parcel's four lifecycle events.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import MonthTransform, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, DoubleType, IntegerType, NestedField, StringType, TimestampType

from src.common.catalog import get_catalog, get_or_create_table, overwrite_table
from src.gold.dimensions import CARRIERS_TABLE_IDENTIFIER

TABLE_IDENTIFIER = "gold.shipments"

RECORD_SCHEMA = Schema(
    NestedField(1, "parcel_id", IntegerType(), required=True),
    NestedField(2, "tracking_number", StringType(), required=True),
    NestedField(3, "organisation_id", IntegerType(), required=True),
    NestedField(4, "carrier_code", StringType(), required=True),
    NestedField(5, "service_level", StringType(), required=False),
    NestedField(6, "destination_country", StringType(), required=False),
    NestedField(7, "destination_postcode", StringType(), required=False),
    NestedField(8, "created_at", TimestampType(), required=True),
    NestedField(9, "handed_over_at", TimestampType(), required=False),
    NestedField(10, "accepted_at", TimestampType(), required=False),
    NestedField(11, "delivered_at", TimestampType(), required=False),
    NestedField(12, "latest_carrier_status", StringType(), required=False),
    NestedField(13, "weight_kg", DoubleType(), required=False),
    NestedField(14, "facility_code", StringType(), required=False),
    NestedField(15, "sla_hours", IntegerType(), required=False),
    NestedField(16, "transit_hours", DoubleType(), required=False),
    NestedField(17, "is_delivered", BooleanType(), required=True),
    NestedField(18, "is_on_time", BooleanType(), required=False),
    NestedField(19, "_gold_loaded_at", TimestampType(), required=True),
    NestedField(20, "is_lost", BooleanType(), required=False),
    NestedField(21, "is_returned", BooleanType(), required=False),
)
RECORD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=9, field_id=1000, transform=MonthTransform(), name="handed_over_month")
)

TRACKING_EVENTS_TABLE_IDENTIFIER = "silver.tracking_events"
PARCEL_EVENTS_TABLE_IDENTIFIER = "silver.parcel_events"


@dataclass
class TransformResult:
    rows_loaded: int


def _carrier_events_by_parcel(catalog: Catalog) -> dict[tuple[str, str], dict]:
    """(carrier_code, tracking_number) -> accepted_at/delivered_at/weight_kg
    plus whatever status carried the latest event_time (for parcels still
    in flight, that's the most useful single status to expose)."""
    if not catalog.table_exists(TRACKING_EVENTS_TABLE_IDENTIFIER):
        return {}

    by_key: dict[tuple[str, str], dict] = {}
    for row in catalog.load_table(TRACKING_EVENTS_TABLE_IDENTIFIER).scan().to_arrow().to_pylist():
        key = (row["carrier_code"], row["tracking_number"])
        entry = by_key.setdefault(
            key,
            {
                "accepted_at": None,
                "delivered_at": None,
                "weight_kg": None,
                "latest_status": None,
                "latest_status_time": None,
                "facility_code": None,
            },
        )
        if row["status"] == "ACCEPTED":
            entry["accepted_at"] = row["event_time"]
        elif row["status"] == "DELIVERED":
            entry["delivered_at"] = row["event_time"]
        if row.get("weight_kg") is not None:
            entry["weight_kg"] = row["weight_kg"]
        if entry["latest_status_time"] is None or row["event_time"] > entry["latest_status_time"]:
            entry["latest_status_time"] = row["event_time"]
            entry["latest_status"] = row["status"]
            entry["facility_code"] = row.get("facility_code")
    return by_key


def _parcels(catalog: Catalog) -> dict[int, dict]:
    by_parcel: dict[int, dict] = {}
    for row in catalog.load_table(PARCEL_EVENTS_TABLE_IDENTIFIER).scan().to_arrow().to_pylist():
        parcel = by_parcel.setdefault(
            row["parcel_id"],
            {
                "parcel_id": row["parcel_id"],
                "tracking_number": row["tracking_number"],
                "organisation_id": row["organisation_id"],
                "carrier_code": row["carrier_code"],
                "service_level": row.get("service_level"),
                "destination_country": row.get("destination_country"),
                "destination_postcode": row.get("destination_postcode"),
                "created_at": None,
                "handed_over_at": None,
            },
        )
        if row["event_type"] == "parcel_created":
            parcel["created_at"] = row["occurred_at"]
        elif row["event_type"] == "parcel_handed_over":
            parcel["handed_over_at"] = row["occurred_at"]
    return by_parcel


def build(catalog: Catalog) -> TransformResult:
    table = get_or_create_table(catalog, TABLE_IDENTIFIER, RECORD_SCHEMA, RECORD_PARTITION_SPEC)

    sla_by_carrier: dict[str, int] = {}
    if catalog.table_exists(CARRIERS_TABLE_IDENTIFIER):
        for r in catalog.load_table(CARRIERS_TABLE_IDENTIFIER).scan().to_arrow().to_pylist():
            sla_by_carrier[r["carrier_code"]] = r["sla_hours"]

    carrier_events = _carrier_events_by_parcel(catalog)
    loaded_at = datetime.now(timezone.utc)

    rows = []
    for parcel in _parcels(catalog).values():
        ce = carrier_events.get((parcel["carrier_code"], parcel["tracking_number"]), {})
        accepted_at = ce.get("accepted_at")
        delivered_at = ce.get("delivered_at")
        latest_status = ce.get("latest_status")
        sla_hours = sla_by_carrier.get(parcel["carrier_code"])

        transit_hours = None
        is_on_time = None
        if accepted_at is not None and delivered_at is not None:
            transit_hours = (delivered_at - accepted_at).total_seconds() / 3600
            if sla_hours is not None:
                is_on_time = transit_hours <= sla_hours

        rows.append(
            {
                **parcel,
                "accepted_at": accepted_at,
                "delivered_at": delivered_at,
                "latest_carrier_status": latest_status,
                "weight_kg": ce.get("weight_kg"),
                "facility_code": ce.get("facility_code"),
                "sla_hours": sla_hours,
                "transit_hours": transit_hours,
                "is_delivered": delivered_at is not None,
                "is_on_time": is_on_time,
                "_gold_loaded_at": loaded_at,
                "is_lost": latest_status == "LOST",
                "is_returned": latest_status == "RETURNED",
            }
        )

    overwrite_table(table, rows)
    return TransformResult(rows_loaded=len(rows))


def run() -> TransformResult:
    return build(get_catalog())
