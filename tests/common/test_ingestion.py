import gzip
import json
from pathlib import Path

import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, NestedField, StringType, TimestampType

from src.common.catalog import get_catalog
from src.common.ingestion import already_ingested_files, ingest_jsonl_gz_file, iter_raw_files

TABLE_IDENTIFIER = "bronze.test_source"
QUARANTINE_TABLE_IDENTIFIER = "bronze.test_source_quarantine"

RECORD_SCHEMA = Schema(
    NestedField(1, "value", StringType(), required=False),
    NestedField(2, "source_file", StringType(), required=True),
    NestedField(3, "source_line_no", IntegerType(), required=True),
    NestedField(4, "_bronze_loaded_at", TimestampType(), required=True),
)
RECORD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="source_file")
)


@pytest.fixture
def catalog(tmp_path):
    return get_catalog(warehouse_dir=tmp_path / "warehouse")


def _write_gz(path: Path, lines: list[str]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for line in lines:
            fh.write(line + "\n")


def _row_from_json(obj: dict) -> dict:
    return {"value": obj.get("value")}


def _ingest(catalog, path: Path):
    return ingest_jsonl_gz_file(
        catalog, path, TABLE_IDENTIFIER, QUARANTINE_TABLE_IDENTIFIER, RECORD_SCHEMA, RECORD_PARTITION_SPEC, _row_from_json
    )


def test_iter_raw_files_skips_nothing_by_default(tmp_path):
    (tmp_path / "a.json.gz").touch()

    assert {p.name for p in iter_raw_files(tmp_path, "*.json.gz")} == {"a.json.gz"}


def test_iter_raw_files_skips_named_files(tmp_path):
    (tmp_path / "a.json.gz").touch()
    (tmp_path / "b.json.gz").touch()

    files = {p.name for p in iter_raw_files(tmp_path, "*.json.gz", skip={"a.json.gz"})}

    assert files == {"b.json.gz"}


def test_already_ingested_files_empty_for_nonexistent_tables(catalog):
    assert already_ingested_files(catalog, "bronze.nope", "bronze.nope_quarantine") == set()


def test_already_ingested_files_includes_files_with_only_good_rows(catalog, tmp_path):
    path = tmp_path / "source_a.json.gz"
    _write_gz(path, [json.dumps({"value": "x"})])
    _ingest(catalog, path)

    assert already_ingested_files(catalog, TABLE_IDENTIFIER, QUARANTINE_TABLE_IDENTIFIER) == {"source_a.json.gz"}


def test_already_ingested_files_includes_files_with_only_bad_rows(catalog, tmp_path):
    path = tmp_path / "source_b.json.gz"
    _write_gz(path, ["not-json"])
    _ingest(catalog, path)

    assert already_ingested_files(catalog, TABLE_IDENTIFIER, QUARANTINE_TABLE_IDENTIFIER) == {"source_b.json.gz"}


def test_already_ingested_files_excludes_untouched_files(catalog, tmp_path):
    path = tmp_path / "source_c.json.gz"
    _write_gz(path, [json.dumps({"value": "x"})])
    _ingest(catalog, path)

    known = already_ingested_files(catalog, TABLE_IDENTIFIER, QUARANTINE_TABLE_IDENTIFIER)

    assert "source_d.json.gz" not in known


def test_second_pass_only_processes_new_files(catalog, tmp_path):
    first = tmp_path / "source_a.json.gz"
    _write_gz(first, [json.dumps({"value": "x"})])
    _ingest(catalog, first)

    second = tmp_path / "source_b.json.gz"
    _write_gz(second, [json.dumps({"value": "y"})])

    skip = already_ingested_files(catalog, TABLE_IDENTIFIER, QUARANTINE_TABLE_IDENTIFIER)
    remaining = [p.name for p in iter_raw_files(tmp_path, "*.json.gz", skip)]

    assert remaining == ["source_b.json.gz"]
