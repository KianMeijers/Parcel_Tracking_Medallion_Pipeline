import gzip
import json
from pathlib import Path

import pytest

from src.bronze.carrier_b import ingest_file
from src.common.catalog import get_catalog

GOOD_LINES = [
    {"barcode": "JD1", "event": {"code": 10, "desc": "Aangenomen"}, "event_time": "2026-05-01 10:00:00", "weight_g": 1200, "depot": "AMS", "_ingested_at": "2026-05-01T10:05:00Z"},
    {"barcode": "JD1", "event": {"code": 45, "desc": "Afgeleverd"}, "event_time": "2026-05-01 18:00:00", "weight_g": 1200, "depot": "AMS", "_ingested_at": "2026-05-01T18:05:00Z"},
]
BAD_LINE = '{"barcode": "JD2", "event": {"cod'


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
    path = tmp_path / "2026-05-01.jsonl.gz"
    _write_gz(path, GOOD_LINES, BAD_LINE)
    return path


def test_ingest_loads_good_rows_and_quarantines_bad_line(catalog, source_file):
    result = ingest_file(catalog, source_file)

    assert result.rows_loaded == 2
    assert result.rows_quarantined == 1

    rows = catalog.load_table("bronze.carrier_b").scan().to_arrow().to_pylist()
    assert {r["event"]["desc"] for r in rows} == {"Aangenomen", "Afgeleverd"}
    assert all(r["barcode"] == "JD1" for r in rows)

    quarantined = catalog.load_table("bronze.carrier_b_quarantine").scan().to_arrow().to_pylist()
    assert quarantined[0]["raw_line"] == BAD_LINE
    assert quarantined[0]["source_line_no"] == 3


def test_rerunning_the_same_file_does_not_duplicate_rows(catalog, source_file):
    ingest_file(catalog, source_file)
    ingest_file(catalog, source_file)

    rows = catalog.load_table("bronze.carrier_b").scan().to_arrow().to_pylist()
    quarantined = catalog.load_table("bronze.carrier_b_quarantine").scan().to_arrow().to_pylist()

    assert len(rows) == 2
    assert len(quarantined) == 1


def test_a_second_source_file_does_not_touch_the_first_files_rows(catalog, source_file, tmp_path):
    ingest_file(catalog, source_file)

    other_path = tmp_path / "2026-05-02.jsonl.gz"
    _write_gz(other_path, [{"barcode": "JD9", "event": {"code": 10, "desc": "Aangenomen"}, "event_time": "2026-05-02 09:00:00", "weight_g": 500, "depot": "RTM", "_ingested_at": "2026-05-02T09:05:00Z"}], BAD_LINE)
    ingest_file(catalog, other_path)

    rows = catalog.load_table("bronze.carrier_b").scan().to_arrow().to_pylist()
    assert {r["source_file"] for r in rows} == {source_file.name, other_path.name}
    assert len(rows) == 3
