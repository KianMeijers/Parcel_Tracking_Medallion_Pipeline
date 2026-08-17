from datetime import datetime, timezone

import pytest
from pyiceberg.partitioning import PartitionSpec

from agent.duckdb_views import GOLD_VIEWS, register_gold_views
from src.common.catalog import get_catalog, get_or_create_table, overwrite_table
from src.gold.dimensions import CARRIERS_SCHEMA, CARRIERS_TABLE_IDENTIFIER, ORGANISATIONS_SCHEMA, ORGANISATIONS_TABLE_IDENTIFIER
from src.gold.shipments import RECORD_PARTITION_SPEC as SHIPMENTS_PARTITION_SPEC
from src.gold.shipments import RECORD_SCHEMA as SHIPMENTS_SCHEMA
from src.gold.shipments import TABLE_IDENTIFIER as SHIPMENTS_TABLE


@pytest.fixture
def catalog(tmp_path):
    return get_catalog(warehouse_dir=tmp_path / "warehouse")


def _shipment_row(parcel_id: int) -> dict:
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
        "accepted_at": None,
        "delivered_at": None,
        "latest_carrier_status": None,
        "weight_kg": None,
        "facility_code": None,
        "sla_hours": None,
        "transit_hours": None,
        "is_delivered": False,
        "is_on_time": None,
        "_gold_loaded_at": now,
        "is_lost": False,
        "is_returned": False,
    }


def _seed_shipments(catalog, rows: list[dict]) -> None:
    table = get_or_create_table(catalog, SHIPMENTS_TABLE, SHIPMENTS_SCHEMA, SHIPMENTS_PARTITION_SPEC)
    overwrite_table(table, rows)


def _seed_carriers(catalog, rows: list[dict]) -> None:
    table = get_or_create_table(catalog, CARRIERS_TABLE_IDENTIFIER, CARRIERS_SCHEMA, PartitionSpec())
    overwrite_table(table, rows)


def _seed_organisations(catalog, rows: list[dict]) -> None:
    table = get_or_create_table(catalog, ORGANISATIONS_TABLE_IDENTIFIER, ORGANISATIONS_SCHEMA, PartitionSpec())
    overwrite_table(table, rows)


def test_registers_all_three_gold_views_as_queryable_by_name(catalog):
    _seed_shipments(catalog, [_shipment_row(1), _shipment_row(2)])
    _seed_carriers(catalog, [{"carrier_code": "carrier_a", "carrier_name": "PostNL", "sla_hours": 24, "countries_served": None}])
    _seed_organisations(catalog, [{"organisation_id": 1, "name": None, "country": None, "plan": None, "created_at": None}])

    con = register_gold_views(catalog)

    for view_name in GOLD_VIEWS:
        con.execute(f"SELECT * FROM {view_name}").fetchall()  # does not raise


def test_skips_a_view_whose_underlying_table_does_not_exist_yet(catalog):
    _seed_shipments(catalog, [_shipment_row(1)])  # dim_carriers/dim_organisations never built

    con = register_gold_views(catalog)

    assert con.execute("SELECT COUNT(*) FROM shipments").fetchone() == (1,)
    with pytest.raises(Exception, match="dim_carriers"):
        con.execute("SELECT * FROM dim_carriers").fetchall()


def test_view_row_counts_match_the_underlying_iceberg_tables(catalog):
    _seed_shipments(catalog, [_shipment_row(1), _shipment_row(2), _shipment_row(3)])

    con = register_gold_views(catalog)

    assert con.execute("SELECT COUNT(*) FROM shipments").fetchone() == (3,)


def test_external_file_access_is_disabled_on_the_connection(catalog, tmp_path):
    _seed_shipments(catalog, [_shipment_row(1)])
    probe_file = tmp_path / "probe.txt"
    probe_file.write_text("secret")

    con = register_gold_views(catalog)

    with pytest.raises(Exception, match="disabled"):
        con.execute(f"SELECT * FROM read_text('{probe_file.as_posix()}')").fetchall()
