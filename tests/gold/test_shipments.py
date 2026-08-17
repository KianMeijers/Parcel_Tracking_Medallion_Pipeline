import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import src.silver.parcel_events as silver_pe
import src.silver.tracking_events as silver_te
from src.common.catalog import get_catalog, get_or_create_table, overwrite_table
from src.gold.dimensions import build_carriers
from src.gold.shipments import build

CARRIERS = [
    {"carrier_code": "carrier_a", "carrier_name": "PostNL", "sla_hours": 24, "countries_served": ["NL"]},
]

BASE_PARCEL_EVENT = {
    "event_type": "parcel_created",
    "parcel_id": 1,
    "tracking_number": "A1",
    "organisation_id": 4471,
    "carrier_code": "carrier_a",
    "service_level": "standard",
    "destination_country": "NL",
    "destination_postcode": "1234AB",
    "source_file": "parcel_events_2026-07-01.jsonl.gz",
    "_silver_loaded_at": datetime.now(timezone.utc),
}

BASE_TRACKING_EVENT = {
    "tracking_number": "A1",
    "carrier_code": "carrier_a",
    "weight_kg": 2.5,
    "facility_code": "AMS",
    "source_file": "carrier_a_20260701.json.gz",
    "_silver_loaded_at": datetime.now(timezone.utc),
}


def _parcel_event(event_id: str, event_type: str, occurred_at: datetime, line_no: int, **overrides) -> dict:
    row = dict(BASE_PARCEL_EVENT, event_id=event_id, event_type=event_type, occurred_at=occurred_at, source_line_no=line_no)
    row.update(overrides)
    return row


def _tracking_event(status: str, event_time: datetime, line_no: int, **overrides) -> dict:
    row = dict(BASE_TRACKING_EVENT, status=status, event_time=event_time, raw_status=status, source_line_no=line_no)
    row.update(overrides)
    return row


def _write_reference_carriers(catalog, tmp_path: Path) -> None:
    ref_dir = tmp_path / "reference"
    ref_dir.mkdir(exist_ok=True)
    with (ref_dir / "carriers.json").open("w", encoding="utf-8") as fh:
        for record in CARRIERS:
            fh.write(json.dumps(record) + "\n")
    build_carriers(catalog, reference_dir=ref_dir)


def _seed_silver(catalog, parcel_events: list[dict], tracking_events: list[dict]) -> None:
    pe_table = get_or_create_table(catalog, silver_pe.TABLE_IDENTIFIER, silver_pe.RECORD_SCHEMA, silver_pe.RECORD_PARTITION_SPEC)
    overwrite_table(pe_table, parcel_events)
    te_table = get_or_create_table(catalog, silver_te.TABLE_IDENTIFIER, silver_te.RECORD_SCHEMA, silver_te.RECORD_PARTITION_SPEC)
    overwrite_table(te_table, tracking_events)


@pytest.fixture
def catalog(tmp_path):
    return get_catalog(warehouse_dir=tmp_path / "warehouse")


def test_computes_transit_hours_and_on_time_flag_from_carrier_possession(catalog, tmp_path):
    _write_reference_carriers(catalog, tmp_path)
    _seed_silver(
        catalog,
        parcel_events=[
            _parcel_event("e1", "parcel_created", datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc), 1),
            _parcel_event("e2", "parcel_handed_over", datetime(2026, 7, 1, 8, 30, tzinfo=timezone.utc), 2),
        ],
        tracking_events=[
            # accepted 1h *after* handover - transit_hours must use this, not handed_over_at
            _tracking_event("ACCEPTED", datetime(2026, 7, 1, 9, 0, tzinfo=timezone.utc), 1),
            _tracking_event("DELIVERED", datetime(2026, 7, 2, 8, 0, tzinfo=timezone.utc), 2),
        ],
    )

    result = build(catalog)

    assert result.rows_loaded == 1
    row = catalog.load_table("gold.shipments").scan().to_arrow().to_pylist()[0]
    assert row["accepted_at"] == datetime(2026, 7, 1, 9, 0)  # TimestampType round-trips as naive UTC
    assert row["transit_hours"] == pytest.approx(23.0)
    assert row["is_delivered"] is True
    assert row["is_on_time"] is True  # 23h <= 24h SLA
    assert row["weight_kg"] == pytest.approx(2.5)


def test_flags_late_delivery_as_not_on_time(catalog, tmp_path):
    _write_reference_carriers(catalog, tmp_path)
    _seed_silver(
        catalog,
        parcel_events=[_parcel_event("e1", "parcel_created", datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc), 1)],
        tracking_events=[
            _tracking_event("ACCEPTED", datetime(2026, 7, 1, 9, 0, tzinfo=timezone.utc), 1),
            _tracking_event("DELIVERED", datetime(2026, 7, 3, 0, 0, tzinfo=timezone.utc), 2),  # 39h later, SLA is 24h
        ],
    )

    build(catalog)

    row = catalog.load_table("gold.shipments").scan().to_arrow().to_pylist()[0]
    assert row["is_on_time"] is False


def test_parcel_never_scanned_by_carrier_still_counts_as_shipped(catalog, tmp_path):
    _write_reference_carriers(catalog, tmp_path)
    _seed_silver(
        catalog,
        parcel_events=[
            _parcel_event("e1", "parcel_created", datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc), 1),
            _parcel_event("e2", "parcel_handed_over", datetime(2026, 7, 1, 8, 30, tzinfo=timezone.utc), 2),
        ],
        tracking_events=[],
    )

    result = build(catalog)

    assert result.rows_loaded == 1
    row = catalog.load_table("gold.shipments").scan().to_arrow().to_pylist()[0]
    assert row["handed_over_at"] is not None  # still a real shipment
    assert row["is_delivered"] is False
    assert row["accepted_at"] is None
    assert row["transit_hours"] is None
    assert row["is_on_time"] is None


def test_flags_lost_parcel_without_resolving_transit_time(catalog, tmp_path):
    _write_reference_carriers(catalog, tmp_path)
    _seed_silver(
        catalog,
        parcel_events=[_parcel_event("e1", "parcel_created", datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc), 1)],
        tracking_events=[
            _tracking_event("ACCEPTED", datetime(2026, 7, 1, 9, 0, tzinfo=timezone.utc), 1),
            _tracking_event("LOST", datetime(2026, 7, 3, 0, 0, tzinfo=timezone.utc), 2),
        ],
    )

    build(catalog)

    row = catalog.load_table("gold.shipments").scan().to_arrow().to_pylist()[0]
    assert row["is_lost"] is True
    assert row["is_returned"] is False
    assert row["is_delivered"] is False
    assert row["transit_hours"] is None
    assert row["is_on_time"] is None


def test_flags_returned_parcel(catalog, tmp_path):
    _write_reference_carriers(catalog, tmp_path)
    _seed_silver(
        catalog,
        parcel_events=[_parcel_event("e1", "parcel_created", datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc), 1)],
        tracking_events=[
            _tracking_event("ACCEPTED", datetime(2026, 7, 1, 9, 0, tzinfo=timezone.utc), 1),
            _tracking_event("RETURNED", datetime(2026, 7, 3, 0, 0, tzinfo=timezone.utc), 2),
        ],
    )

    build(catalog)

    row = catalog.load_table("gold.shipments").scan().to_arrow().to_pylist()[0]
    assert row["is_returned"] is True
    assert row["is_lost"] is False
    assert row["is_delivered"] is False


def test_rerunning_the_transform_does_not_duplicate_rows(catalog, tmp_path):
    _write_reference_carriers(catalog, tmp_path)
    _seed_silver(
        catalog,
        parcel_events=[_parcel_event("e1", "parcel_created", datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc), 1)],
        tracking_events=[_tracking_event("ACCEPTED", datetime(2026, 7, 1, 9, 0, tzinfo=timezone.utc), 1)],
    )

    build(catalog)
    build(catalog)

    rows = catalog.load_table("gold.shipments").scan().to_arrow().to_pylist()
    assert len(rows) == 1
