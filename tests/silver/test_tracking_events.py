import gzip
import json
from pathlib import Path

import pytest

from src.bronze.carrier_a import ingest_file as ingest_carrier_a
from src.bronze.carrier_b import ingest_file as ingest_carrier_b
from src.bronze.carrier_c import ingest_file as ingest_carrier_c
from src.common.catalog import get_catalog
from src.silver.tracking_events import build


def _write_gz(path: Path, records: list[dict]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")


@pytest.fixture
def catalog(tmp_path):
    return get_catalog(warehouse_dir=tmp_path / "warehouse")


def _rows_by_tracking_number(catalog) -> dict:
    return {r["tracking_number"]: r for r in catalog.load_table("silver.tracking_events").scan().to_arrow().to_pylist()}


def test_unifies_status_and_weight_across_all_three_carriers(catalog, tmp_path):
    a_path = tmp_path / "carrier_a_20260501.json.gz"
    _write_gz(a_path, [{"tracking_no": "A1", "status": "DELIVERED", "ts": "2026-05-01T18:00:00Z", "weight_kg": 1.2, "hub": "AMS"}])
    ingest_carrier_a(catalog, a_path)

    b_path = tmp_path / "2026-05-01.jsonl.gz"
    _write_gz(b_path, [{"barcode": "B1", "event": {"code": 45, "desc": "Afgeleverd"}, "event_time": "2026-05-01 18:00:00", "weight_g": 1200, "depot": "AMS"}])
    ingest_carrier_b(catalog, b_path)

    c_path = tmp_path / "dump-1777593600.json.gz"
    _write_gz(c_path, [{"ref": "C1", "st": 3, "t": 1777660336768, "dims": {"weight": 1.56}}])
    ingest_carrier_c(catalog, c_path)

    result = build(catalog)
    assert result.rows_loaded == 3
    assert result.rows_quarantined == 0

    rows = _rows_by_tracking_number(catalog)
    assert rows["A1"]["status"] == rows["B1"]["status"] == rows["C1"]["status"] == "DELIVERED"
    assert rows["A1"]["weight_kg"] == pytest.approx(1.2)
    assert rows["B1"]["weight_kg"] == pytest.approx(1.2)  # 1200g -> 1.2kg
    assert rows["C1"]["weight_kg"] == pytest.approx(1.56)


def test_carrier_c_handles_both_dims_weight_shapes(catalog, tmp_path):
    c_path = tmp_path / "dump-1777593600.json.gz"
    _write_gz(
        c_path,
        [
            {"ref": "C1", "st": 0, "t": 1777660336768, "dims": {"weight": 1.56}},
            {"ref": "C2", "st": 0, "t": 1777660336768, "dims": {"weight": {"v": 1.92, "u": "kg"}}},
        ],
    )
    ingest_carrier_c(catalog, c_path)

    build(catalog)

    rows = _rows_by_tracking_number(catalog)
    assert rows["C1"]["weight_kg"] == pytest.approx(1.56)
    assert rows["C2"]["weight_kg"] == pytest.approx(1.92)


def test_collapses_webhook_retries_into_one_row(catalog, tmp_path):
    a_path = tmp_path / "carrier_a_20260501.json.gz"
    _write_gz(
        a_path,
        [
            {"tracking_no": "A1", "status": "ACCEPTED", "ts": "2026-05-01T10:00:00Z", "weight_kg": 1.0, "hub": "AMS"},
            {"tracking_no": "A1", "status": "ACCEPTED", "ts": "2026-05-01T10:00:00Z", "weight_kg": 1.0, "hub": "AMS"},
        ],
    )
    ingest_carrier_a(catalog, a_path)

    result = build(catalog)

    assert result.rows_loaded == 1


def test_quarantines_semantically_bad_rows(catalog, tmp_path):
    a_path = tmp_path / "carrier_a_20260501.json.gz"
    _write_gz(
        a_path,
        [
            {"tracking_no": None, "status": "ACCEPTED", "ts": "2026-05-01T10:00:00Z", "weight_kg": 1.0, "hub": "AMS"},
            {"tracking_no": "A2", "status": "ACCEPTED", "ts": "not-a-timestamp", "weight_kg": 1.0, "hub": "AMS"},
            {"tracking_no": "A3", "status": "SOMETHING_WEIRD", "ts": "2026-05-01T10:00:00Z", "weight_kg": 1.0, "hub": "AMS"},
        ],
    )
    ingest_carrier_a(catalog, a_path)

    result = build(catalog)

    assert result.rows_loaded == 0
    assert result.rows_quarantined == 3

    reasons = {
        r["reason"].split(":")[0] for r in catalog.load_table("silver.tracking_events_quarantine").scan().to_arrow().to_pylist()
    }
    assert reasons == {"missing_tracking_number", "unparseable_event_time", "unrecognized_status"}


def test_rerunning_the_transform_does_not_duplicate_rows(catalog, tmp_path):
    a_path = tmp_path / "carrier_a_20260501.json.gz"
    _write_gz(a_path, [{"tracking_no": "A1", "status": "ACCEPTED", "ts": "2026-05-01T10:00:00Z", "weight_kg": 1.0, "hub": "AMS"}])
    ingest_carrier_a(catalog, a_path)

    build(catalog)
    build(catalog)

    rows = catalog.load_table("silver.tracking_events").scan().to_arrow().to_pylist()
    assert len(rows) == 1


def test_completes_cases_as_new_bronze_files_land(catalog, tmp_path):
    """A case that looks incomplete today (no DELIVERED yet) should show up
    complete once tomorrow's file lands and the transform reruns - no merge
    logic needed, just a full recompute over the bigger bronze table."""
    day1 = tmp_path / "carrier_a_20260501.json.gz"
    _write_gz(day1, [{"tracking_no": "A1", "status": "ACCEPTED", "ts": "2026-05-01T10:00:00Z", "weight_kg": 1.0, "hub": "AMS"}])
    ingest_carrier_a(catalog, day1)
    build(catalog)

    rows = catalog.load_table("silver.tracking_events").scan().to_arrow().to_pylist()
    assert {r["status"] for r in rows} == {"ACCEPTED"}

    day2 = tmp_path / "carrier_a_20260502.json.gz"
    _write_gz(day2, [{"tracking_no": "A1", "status": "DELIVERED", "ts": "2026-05-02T10:00:00Z", "weight_kg": 1.0, "hub": "AMS"}])
    ingest_carrier_a(catalog, day2)
    build(catalog)

    rows = catalog.load_table("silver.tracking_events").scan().to_arrow().to_pylist()
    assert {r["status"] for r in rows} == {"ACCEPTED", "DELIVERED"}
