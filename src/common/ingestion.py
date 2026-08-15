"""Shared mechanics for bronze-layer ingestion of gzipped-JSONL-per-day
sources (one file per carrier/internal-stream per day).

Every source in src/bronze/ differs only in its raw JSON shape (the
`row_from_json` mapping) and its target schema - the read/parse/quarantine/
overwrite loop around that is identical, so it lives here once.
"""

import gzip
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, NestedField, StringType, TimestampType

from src.common.catalog import get_or_create_table, overwrite_table

QUARANTINE_SCHEMA = Schema(
    NestedField(1, "source_file", StringType(), required=True),
    NestedField(2, "source_line_no", IntegerType(), required=True),
    NestedField(3, "raw_line", StringType(), required=True),
    NestedField(4, "error", StringType(), required=True),
    NestedField(5, "_bronze_loaded_at", TimestampType(), required=True),
)
QUARANTINE_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="source_file")
)


@dataclass
class IngestResult:
    source_file: str
    rows_loaded: int
    rows_quarantined: int


def iter_raw_files(raw_dir: Path, glob_pattern: str) -> Iterator[Path]:
    return iter(sorted(raw_dir.glob(glob_pattern)))


def _parse_json_line(line: str) -> tuple[dict | None, str | None]:
    try:
        return json.loads(line), None
    except json.JSONDecodeError as exc:
        return None, str(exc)


def ingest_jsonl_gz_file(
    catalog: Catalog,
    path: Path,
    table_identifier: str,
    quarantine_table_identifier: str,
    record_schema: Schema,
    partition_spec: PartitionSpec,
    row_from_json: Callable[[dict], dict],
) -> IngestResult:
    """Parse `path` line by line, map each JSON object to a row via
    `row_from_json`, and quarantine lines that fail to parse as JSON at all
    (a structural problem - semantic validation is a silver-layer concern).
    Idempotent: replaces this file's prior slice of both tables atomically.
    """
    table = get_or_create_table(catalog, table_identifier, record_schema, partition_spec)
    quarantine_table = get_or_create_table(
        catalog, quarantine_table_identifier, QUARANTINE_SCHEMA, QUARANTINE_PARTITION_SPEC
    )

    source_file = path.name
    loaded_at = datetime.now(timezone.utc)
    good_rows: list[dict] = []
    bad_rows: list[dict] = []

    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line_no, raw_line in enumerate(fh, start=1):
            line = raw_line.rstrip("\n")
            if not line.strip():
                continue
            obj, error = _parse_json_line(line)
            if error is not None:
                bad_rows.append(
                    {
                        "source_file": source_file,
                        "source_line_no": line_no,
                        "raw_line": line,
                        "error": error,
                        "_bronze_loaded_at": loaded_at,
                    }
                )
                continue
            row = row_from_json(obj)
            row["source_file"] = source_file
            row["source_line_no"] = line_no
            row["_bronze_loaded_at"] = loaded_at
            good_rows.append(row)

    overwrite_filter = f"source_file == '{source_file}'"
    overwrite_table(table, good_rows, overwrite_filter)
    overwrite_table(quarantine_table, bad_rows, overwrite_filter)

    return IngestResult(source_file=source_file, rows_loaded=len(good_rows), rows_quarantined=len(bad_rows))
