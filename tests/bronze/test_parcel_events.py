import gzip
import json
from pathlib import Path

import pytest

from src.bronze.parcel_events import ingest_file
from src.common.catalog import get_catalog

GOOD_LINES = [
    {
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
        "_ingested_at": "2026-05-01T03:47:53Z",
    },
    {
        "event_id": "int-1-label_printed",
        "event_type": "label_printed",
        "parcel_id": 1,
        "tracking_number": "JD1",
        "organisation_id": 4400,
        "carrier_code": "carrier_b",
        "service_level": "standard",
        "destination_country": "NL",
        "destination_postcode": "1234 AB",
        "recipient_email": "test@example.com",
        "occurred_at": "2026-05-01T03:48:34Z",
        "_ingested_at": "2026-05-01T03:48:50Z",
    },
]
BAD_LINE = '{"event_id": "int-2-parcel_crea'


def _write_gz(path: Path, good_lines: list[dict], bad_line: str) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for record in good_lines:
            fh.write(json.dumps(record) + "\n")
        fh.write(bad_line + "\n")


@pytest.fixture
def catalog(tmp_path):
    return get_catalog(warehouse_dir=tmp_path / "warehouse")


@pytest.fixture
def source_file(tmp_path) -> Path:
    path = tmp_path / "parcel_events_2026-05-01.jsonl.gz"
    _write_gz(path, GOOD_LINES, BAD_LINE)
    return path


def test_ingest_loads_good_rows_and_quarantines_bad_line(catalog, source_file):
    result = ingest_file(catalog, source_file)

    assert result.rows_loaded == 2
    assert result.rows_quarantined == 1

    rows = catalog.load_table("bronze.parcel_events").scan().to_arrow().to_pylist()
    assert {r["event_type"] for r in rows} == {"parcel_created", "label_printed"}
    assert all(r["parcel_id"] == 1 for r in rows)

    quarantined = catalog.load_table("bronze.parcel_events_quarantine").scan().to_arrow().to_pylist()
    assert quarantined[0]["raw_line"] == BAD_LINE
    assert quarantined[0]["source_line_no"] == 3


def test_rerunning_the_same_file_does_not_duplicate_rows(catalog, source_file):
    ingest_file(catalog, source_file)
    ingest_file(catalog, source_file)

    rows = catalog.load_table("bronze.parcel_events").scan().to_arrow().to_pylist()
    quarantined = catalog.load_table("bronze.parcel_events_quarantine").scan().to_arrow().to_pylist()

    assert len(rows) == 2
    assert len(quarantined) == 1


def test_a_second_source_file_does_not_touch_the_first_files_rows(catalog, source_file, tmp_path):
    ingest_file(catalog, source_file)

    other_record = dict(GOOD_LINES[0], event_id="int-9-parcel_created", parcel_id=9, occurred_at="2026-05-02T09:00:00Z")
    other_path = tmp_path / "parcel_events_2026-05-02.jsonl.gz"
    _write_gz(other_path, [other_record], BAD_LINE)
    ingest_file(catalog, other_path)

    rows = catalog.load_table("bronze.parcel_events").scan().to_arrow().to_pylist()
    assert {r["source_file"] for r in rows} == {source_file.name, other_path.name}
    assert len(rows) == 3
