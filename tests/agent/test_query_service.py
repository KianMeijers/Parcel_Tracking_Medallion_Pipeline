from datetime import datetime, timezone

import pytest

from agent.duckdb_views import register_gold_views
from agent.query_service import DEFAULT_ROW_LIMIT, run_query
from agent.sql_guard import QueryNotAllowedError
from src.common.catalog import get_catalog, get_or_create_table, overwrite_table
from src.gold.dimensions import CARRIERS_SCHEMA, CARRIERS_TABLE_IDENTIFIER
from src.gold.shipments import RECORD_PARTITION_SPEC as SHIPMENTS_PARTITION_SPEC
from src.gold.shipments import RECORD_SCHEMA as SHIPMENTS_SCHEMA
from src.gold.shipments import TABLE_IDENTIFIER as SHIPMENTS_TABLE
from pyiceberg.partitioning import PartitionSpec


@pytest.fixture
def catalog(tmp_path):
    return get_catalog(warehouse_dir=tmp_path / "warehouse")


def _shipment_row(parcel_id: int, carrier_code: str = "carrier_a") -> dict:
    now = datetime.now(timezone.utc)
    return {
        "parcel_id": parcel_id,
        "tracking_number": f"T{parcel_id}",
        "organisation_id": 1,
        "carrier_code": carrier_code,
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


def test_run_query_returns_rows_for_a_simple_select(catalog):
    _seed_shipments(catalog, [_shipment_row(1), _shipment_row(2)])
    con = register_gold_views(catalog)

    result = run_query(con, "SELECT parcel_id FROM shipments ORDER BY parcel_id")

    assert result.columns == ["parcel_id"]
    assert result.rows == [(1,), (2,)]
    assert result.row_count == 2
    assert result.truncated is False


def test_run_query_joins_shipments_with_dim_carriers(catalog):
    _seed_shipments(catalog, [_shipment_row(1, carrier_code="carrier_a")])
    _seed_carriers(catalog, [{"carrier_code": "carrier_a", "carrier_name": "PostNL", "sla_hours": 24, "countries_served": None}])
    con = register_gold_views(catalog)

    result = run_query(
        con,
        "SELECT s.parcel_id, c.carrier_name FROM shipments s JOIN dim_carriers c ON s.carrier_code = c.carrier_code",
    )

    assert result.rows == [(1, "PostNL")]


def test_run_query_truncates_results_at_the_row_cap(catalog):
    _seed_shipments(catalog, [_shipment_row(i) for i in range(DEFAULT_ROW_LIMIT + 1)])
    con = register_gold_views(catalog)

    result = run_query(con, "SELECT parcel_id FROM shipments")

    assert result.truncated is True
    assert result.row_count == DEFAULT_ROW_LIMIT


def test_run_query_does_not_truncate_when_under_the_cap(catalog):
    _seed_shipments(catalog, [_shipment_row(i) for i in range(5)])
    con = register_gold_views(catalog)

    result = run_query(con, "SELECT parcel_id FROM shipments")

    assert result.truncated is False
    assert result.row_count == 5


def test_run_query_rejects_drop_table(catalog):
    _seed_shipments(catalog, [_shipment_row(1)])
    con = register_gold_views(catalog)

    with pytest.raises(QueryNotAllowedError):
        run_query(con, "DROP TABLE shipments")


def test_run_query_rejects_multi_statement_injection(catalog):
    _seed_shipments(catalog, [_shipment_row(1)])
    con = register_gold_views(catalog)

    with pytest.raises(QueryNotAllowedError):
        run_query(con, "SELECT * FROM shipments; DROP TABLE shipments;")


def test_run_query_surfaces_a_duckdb_syntax_error_readably(catalog):
    _seed_shipments(catalog, [_shipment_row(1)])
    con = register_gold_views(catalog)

    with pytest.raises(Exception, match="does_not_exist"):
        run_query(con, "SELECT does_not_exist FROM shipments")
