import gzip
import json
from pathlib import Path

import pytest

from src.bronze.parcel_events import ingest_file
from src.common.catalog import get_catalog
from src.silver.parcel_events import build

GOOD_EVENT = {
    "event_id": "int-1-parcel_created",
    "event_type": "parcel_created",
    "parcel_id": 1,
    "tracking_number": "JD1",
    "organisation_id": 4400,
    "carrier_code": "carrier_b",
    "service_level": "standard",
    "destination_country": "NL",
    "destination_postcode": "1234 AB",
    "recipient_email": "test@example.com",
    "occurred_at": "2026-05-01T03:47:34Z",
}


def _write_gz(path: Path, records: list[dict]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")


@pytest.fixture
def catalog(tmp_path):
    return get_catalog(warehouse_dir=tmp_path / "warehouse")


@pytest.fixture
def source_file(tmp_path) -> Path:
    return tmp_path / "parcel_events_2026-05-01.jsonl.gz"


def test_cleans_and_types_good_rows_and_drops_email(catalog, source_file):
    _write_gz(source_file, [GOOD_EVENT])
    ingest_file(catalog, source_file)

    result = build(catalog)

    assert result.rows_loaded == 1
    assert result.rows_quarantined == 0

    rows = catalog.load_table("silver.parcel_events").scan().to_arrow().to_pylist()
    assert rows[0]["tracking_number"] == "JD1"
    assert rows[0]["organisation_id"] == 4400
    assert "recipient_email" not in rows[0]


def test_quarantines_rows_missing_a_required_field(catalog, source_file):
    bad = dict(GOOD_EVENT, event_id="int-2-parcel_created", parcel_id=None)
    _write_gz(source_file, [bad])
    ingest_file(catalog, source_file)

    result = build(catalog)

    assert result.rows_loaded == 0
    assert result.rows_quarantined == 1

    reasons = [r["reason"] for r in catalog.load_table("silver.parcel_events_quarantine").scan().to_arrow().to_pylist()]
    assert reasons == ["missing_fields:parcel_id"]


def test_quarantines_unparseable_occurred_at(catalog, source_file):
    bad = dict(GOOD_EVENT, event_id="int-3-parcel_created", occurred_at="not-a-timestamp")
    _write_gz(source_file, [bad])
    ingest_file(catalog, source_file)

    result = build(catalog)

    assert result.rows_loaded == 0
    assert result.rows_quarantined == 1


def test_dedups_retried_events_on_event_id(catalog, source_file):
    _write_gz(source_file, [GOOD_EVENT, GOOD_EVENT])
    ingest_file(catalog, source_file)

    result = build(catalog)

    assert result.rows_loaded == 1


def test_rerunning_the_transform_does_not_duplicate_rows(catalog, source_file):
    _write_gz(source_file, [GOOD_EVENT])
    ingest_file(catalog, source_file)

    build(catalog)
    build(catalog)

    rows = catalog.load_table("silver.parcel_events").scan().to_arrow().to_pylist()
    assert len(rows) == 1
