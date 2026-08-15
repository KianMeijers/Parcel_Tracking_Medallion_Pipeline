import gzip
import json
from pathlib import Path

import pytest

from src.bronze.carrier_c import ingest_file
from src.common.catalog import get_catalog

GOOD_LINES = [
    {"ref": "CC-1", "st": 0, "t": 1777660336768, "dims": {"weight": 1.56}, "_ingested_at": "2026-05-01T18:50:25Z"},
    {"ref": "CC-2", "st": 2, "t": 1781398635733, "dims": {"weight": {"v": 1.92, "u": "kg"}}, "_ingested_at": "2026-06-14T01:28:03Z"},
]
BAD_LINE = '{"ref": "CC-3", "st": 0, "t'


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
    path = tmp_path / "dump-1777593600.json.gz"
    _write_gz(path, GOOD_LINES, BAD_LINE)
    return path


def test_ingest_loads_both_dims_weight_shapes_and_quarantines_bad_line(catalog, source_file):
    result = ingest_file(catalog, source_file)

    assert result.rows_loaded == 2
    assert result.rows_quarantined == 1

    rows = {r["ref"]: r for r in catalog.load_table("bronze.carrier_c").scan().to_arrow().to_pylist()}
    assert json.loads(rows["CC-1"]["dims"]) == {"weight": 1.56}
    assert json.loads(rows["CC-2"]["dims"]) == {"weight": {"v": 1.92, "u": "kg"}}
    assert rows["CC-1"]["t"] == 1777660336768  # fits only because t is LongType, not IntegerType

    quarantined = catalog.load_table("bronze.carrier_c_quarantine").scan().to_arrow().to_pylist()
    assert quarantined[0]["raw_line"] == BAD_LINE
    assert quarantined[0]["source_line_no"] == 3


def test_rerunning_the_same_file_does_not_duplicate_rows(catalog, source_file):
    ingest_file(catalog, source_file)
    ingest_file(catalog, source_file)

    rows = catalog.load_table("bronze.carrier_c").scan().to_arrow().to_pylist()
    quarantined = catalog.load_table("bronze.carrier_c_quarantine").scan().to_arrow().to_pylist()

    assert len(rows) == 2
    assert len(quarantined) == 1


def test_a_second_source_file_does_not_touch_the_first_files_rows(catalog, source_file, tmp_path):
    ingest_file(catalog, source_file)

    other_path = tmp_path / "dump-1777680000.json.gz"
    _write_gz(other_path, [{"ref": "CC-9", "st": 1, "t": 1777680000000, "dims": {"weight": 0.5}, "_ingested_at": "2026-05-02T09:05:00Z"}], BAD_LINE)
    ingest_file(catalog, other_path)

    rows = catalog.load_table("bronze.carrier_c").scan().to_arrow().to_pylist()
    assert {r["source_file"] for r in rows} == {source_file.name, other_path.name}
    assert len(rows) == 3
