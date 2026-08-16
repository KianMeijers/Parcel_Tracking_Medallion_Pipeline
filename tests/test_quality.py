from datetime import datetime, timezone

import pytest
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

from src.common.catalog import get_catalog, get_or_create_table, overwrite_table
from src.gold.shipments import RECORD_PARTITION_SPEC as GOLD_SHIPMENTS_PARTITION_SPEC
from src.gold.shipments import RECORD_SCHEMA as GOLD_SHIPMENTS_SCHEMA
from src.gold.shipments import TABLE_IDENTIFIER as GOLD_SHIPMENTS_TABLE
from src.quality import (
    BRONZE_TABLES,
    SILVER_TABLES,
    DataQualityError,
    check_bronze_quarantine_rates,
    check_gold_shipments,
    check_silver_quarantine_rates,
)

SIMPLE_SCHEMA = Schema(NestedField(1, "id", StringType(), required=True))


@pytest.fixture
def catalog(tmp_path):
    return get_catalog(warehouse_dir=tmp_path / "warehouse")


def _seed_count(catalog, identifier: str, row_count: int) -> None:
    table = get_or_create_table(catalog, identifier, SIMPLE_SCHEMA, PartitionSpec())
    overwrite_table(table, [{"id": str(i)} for i in range(row_count)])


def _seed_all_bronze_clean(catalog) -> None:
    for table, quarantine in BRONZE_TABLES:
        _seed_count(catalog, table, 100)
        _seed_count(catalog, quarantine, 1)


def _seed_all_silver_clean(catalog) -> None:
    for table, quarantine in SILVER_TABLES:
        _seed_count(catalog, table, 100)
        _seed_count(catalog, quarantine, 1)


def test_bronze_quarantine_rates_pass_under_threshold(catalog):
    _seed_all_bronze_clean(catalog)

    results = check_bronze_quarantine_rates(catalog)

    assert len(results) == len(BRONZE_TABLES)
    assert all(r.rate < 0.05 for r in results)


def test_bronze_quarantine_rates_fail_over_threshold(catalog):
    _seed_all_bronze_clean(catalog)
    _seed_count(catalog, "bronze.carrier_a", 90)
    _seed_count(catalog, "bronze.carrier_a_quarantine", 10)  # 10% > 5% threshold

    with pytest.raises(DataQualityError, match="bronze.carrier_a"):
        check_bronze_quarantine_rates(catalog)


def test_silver_quarantine_rates_fail_over_threshold(catalog):
    _seed_all_silver_clean(catalog)
    _seed_count(catalog, "silver.tracking_events", 50)
    _seed_count(catalog, "silver.tracking_events_quarantine", 50)  # 50% > 5% threshold

    with pytest.raises(DataQualityError, match="silver.tracking_events"):
        check_silver_quarantine_rates(catalog)


def _shipment_row(parcel_id: int, transit_hours: float | None = None) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "parcel_id": parcel_id,
        "tracking_number": f"T{parcel_id}",
        "organisation_id": 1,
        "carrier_code": "carrier_a",
        "service_level": None,
        "destination_country": None,
        "destination_postcode": None,
        "created_at": now,
        "handed_over_at": None,
        "accepted_at": now,
        "delivered_at": now,
        "latest_carrier_status": None,
        "weight_kg": None,
        "facility_code": None,
        "sla_hours": None,
        "transit_hours": transit_hours,
        "is_delivered": True,
        "is_on_time": None,
        "_gold_loaded_at": now,
    }


def _seed_shipments(catalog, rows: list[dict]) -> None:
    table = get_or_create_table(catalog, GOLD_SHIPMENTS_TABLE, GOLD_SHIPMENTS_SCHEMA, GOLD_SHIPMENTS_PARTITION_SPEC)
    overwrite_table(table, rows)


def test_gold_shipments_passes_clean_data(catalog):
    _seed_shipments(catalog, [_shipment_row(1), _shipment_row(2)])

    assert check_gold_shipments(catalog) == {"row_count": 2}


def test_gold_shipments_catches_duplicate_parcel_id(catalog):
    _seed_shipments(catalog, [_shipment_row(1), _shipment_row(1)])

    with pytest.raises(DataQualityError, match="duplicate parcel_id"):
        check_gold_shipments(catalog)


def test_gold_shipments_catches_negative_transit_hours(catalog):
    _seed_shipments(catalog, [_shipment_row(1, transit_hours=-5.0)])

    with pytest.raises(DataQualityError, match="negative transit_hours"):
        check_gold_shipments(catalog)


def test_gold_shipments_catches_empty_table(catalog):
    get_or_create_table(catalog, GOLD_SHIPMENTS_TABLE, GOLD_SHIPMENTS_SCHEMA, GOLD_SHIPMENTS_PARTITION_SPEC)

    with pytest.raises(DataQualityError, match="table is empty"):
        check_gold_shipments(catalog)
