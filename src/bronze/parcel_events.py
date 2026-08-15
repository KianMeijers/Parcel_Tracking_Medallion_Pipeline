"""Bronze-layer ingestion for parcel_events - internal parcel
lifecycle stream 

Source: one gzipped JSON-lines file per day under data/raw/parcel_events/,
e.g. parcel_events_2026-05-01.jsonl.gz.

Design choices:
- occurred_at is kept as a raw string, even though every observed value is
  well-formed ISO 8601 UTC. 
- recipient_email lands in bronze unmasked, as raw as everything else
- Only lines that fail to parse as JSON at all are quarantined - a
  structural problem, not a semantic one, same policy as the carriers.
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
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)

from src.common.catalog import ensure_namespace, get_catalog

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" / "parcel_events"

TABLE_IDENTIFIER = "bronze.parcel_events"
QUARANTINE_TABLE_IDENTIFIER = "bronze.parcel_events_quarantine"

RECORD_SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=False),
    NestedField(2, "event_type", StringType(), required=False),
    NestedField(3, "parcel_id", IntegerType(), required=False),
    NestedField(4, "tracking_number", StringType(), required=False),
    NestedField(5, "organisation_id", IntegerType(), required=False),
    NestedField(6, "carrier_code", StringType(), required=False),
    NestedField(7, "service_level", StringType(), required=False),
    NestedField(8, "destination_country", StringType(), required=False),
    NestedField(9, "destination_postcode", StringType(), required=False),
    NestedField(10, "recipient_email", StringType(), required=False),
    NestedField(11, "occurred_at", StringType(), required=False),
    NestedField(12, "source_ingested_at", StringType(), required=False),
    NestedField(13, "source_file", StringType(), required=True),
    NestedField(14, "source_line_no", IntegerType(), required=True),
    NestedField(15, "_bronze_loaded_at", TimestampType(), required=True),
)
RECORD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=13, field_id=1000, transform=IdentityTransform(), name="source_file")
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
    # Use the table's own committed schema, not a module-level Schema constant -
    # see src/bronze/carrier_a.py's _replace_source_file for why.
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
                    "event_id": obj.get("event_id"),
                    "event_type": obj.get("event_type"),
                    "parcel_id": obj.get("parcel_id"),
                    "tracking_number": obj.get("tracking_number"),
                    "organisation_id": obj.get("organisation_id"),
                    "carrier_code": obj.get("carrier_code"),
                    "service_level": obj.get("service_level"),
                    "destination_country": obj.get("destination_country"),
                    "destination_postcode": obj.get("destination_postcode"),
                    "recipient_email": obj.get("recipient_email"),
                    "occurred_at": obj.get("occurred_at"),
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
