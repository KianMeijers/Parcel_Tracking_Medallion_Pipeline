"""Bronze-layer ingestion for carrier_b tracking events.

Source: one gzipped JSON-lines file per day under data/raw/carrier_b/, e.g.
2026-05-01.jsonl.gz. The filename only reflects when the file was
dumped, not when the events inside happened, so it is used purely as a
lineage / idempotency key.

Design choices:
- Fields are kept close to their raw shape. `event_time` stays a string
  (it has no timezone marker, e.g. "2026-05-01 12:37:37" - assuming UTC is
  a silver-layer decision, not a bronze one). `weight_g` stays named and
  valued in grams, not converted to kg - unit normalization across carriers
  is explicitly a silver concern per the project spec. The nested `event`
  object (event.code, event.desc) is kept nested rather than flattened,
  since that's how the carrier actually sends it.
- Only lines that fail to parse as JSON at all are quarantined here - a
  structural problem, not a semantic one. Carriers retry webhooks, so
  repeated (barcode, event.code, event_time) triples across a file are expected raw
  duplicates and are intentionally not deduped in bronze; that happens in
  silver.
- Idempotency: each load is scoped to a single source file and replaces any
  rows previously loaded from that file (both the data table and its
  quarantine sibling) via an atomic Iceberg overwrite, so reprocessing the
  same file twice never duplicates rows.
"""

import gzip
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
    StructType
)

from src.common.catalog import ensure_namespace, get_catalog

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" / "carrier_b"

TABLE_IDENTIFIER = "bronze.carrier_b"
QUARANTINE_TABLE_IDENTIFIER = "bronze.carrier_b_quarantine"

RECORD_SCHEMA = Schema(
    NestedField(1, "barcode", StringType(), required=False),
    NestedField(2, "event", StructType(
        NestedField(3, "code", IntegerType(), required=False),
        NestedField(4, "desc", StringType(), required=False),
    ), required=False),
    NestedField(5, "event_time", StringType(), required=False),
    NestedField(6, "weight_g", DoubleType(), required=False),
    NestedField(7, "depot", StringType(), required=False),
    NestedField(8, "source_ingested_at", StringType(), required=False),
    NestedField(9, "source_file", StringType(), required=True),
    NestedField(10, "source_line_no", IntegerType(), required=True),
    NestedField(11, "_bronze_loaded_at", TimestampType(), required=True),
)

RECORD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=9, field_id=1000, transform=IdentityTransform(), name="source_file")
)

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


def _get_or_create_table(catalog: Catalog, identifier: str, schema: Schema, partition_spec: PartitionSpec) -> Table:
    namespace = identifier.split(".")[0]
    ensure_namespace(catalog, namespace)
    if catalog.table_exists(identifier):
        return catalog.load_table(identifier)
    return catalog.create_table(identifier, schema=schema, partition_spec=partition_spec)


def _parse_line(line: str) -> tuple[dict | None, str | None]:
    try:
        return json.loads(line), None
    except json.JSONDecodeError as exc:
        return None, str(exc)


def _replace_source_file(table: Table, source_file: str, rows: list[dict]) -> None:
    arrow_table = pa.Table.from_pylist(rows, schema=table.schema().as_arrow())
    table.overwrite(arrow_table, overwrite_filter=f"source_file == '{source_file}'")


def iter_raw_files(raw_dir: Path = RAW_DIR) -> Iterator[Path]:
    return iter(sorted(raw_dir.glob("*.jsonl.gz")))


def ingest_file(catalog: Catalog, path: Path) -> IngestResult:
    table = _get_or_create_table(catalog, TABLE_IDENTIFIER, RECORD_SCHEMA, RECORD_PARTITION_SPEC)
    quarantine_table = _get_or_create_table(
        catalog, QUARANTINE_TABLE_IDENTIFIER, QUARANTINE_SCHEMA, QUARANTINE_PARTITION_SPEC
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
            obj, error = _parse_line(line)
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
            good_rows.append(
                {
                    "barcode": obj.get("barcode"),
                    "event": obj.get("event"),
                    "event_time": obj.get("event_time"),
                    "weight_g": obj.get("weight_g"),
                    "depot": obj.get("depot"),
                    "source_ingested_at": obj.get("_ingested_at"),
                    "source_file": source_file,
                    "source_line_no": line_no,
                    "_bronze_loaded_at": loaded_at,
                }
            )

    _replace_source_file(table, source_file, good_rows)
    _replace_source_file(quarantine_table, source_file, bad_rows)

    return IngestResult(source_file=source_file, rows_loaded=len(good_rows), rows_quarantined=len(bad_rows))


def ingest_all(raw_dir: Path = RAW_DIR) -> list[IngestResult]:
    catalog = get_catalog()
    return [ingest_file(catalog, path) for path in iter_raw_files(raw_dir)]
