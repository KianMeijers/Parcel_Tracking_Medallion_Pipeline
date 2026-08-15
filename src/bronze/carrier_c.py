"""Bronze-layer ingestion for carrier_c tracking events.

Source: one gzipped JSON-lines file per day under data/raw/carrier_c/, e.g.
dump-1784160000.json.gz (an epoch-seconds dump timestamp, not an event
date). The filename only reflects when the file was dumped, not when the
events inside happened, so it is used purely as a lineage / idempotency
key.

Design choices:
- `t` is epoch *milliseconds* (13 digits) - large enough to overflow a
  32-bit Iceberg IntegerType, so it's typed as LongType.
- `st` is a bare integer status code (0-4 seen in the data). Nothing in
  the reference data maps what these mean, so it's kept as a raw int
  rather than guessed at
- `dims` is kept as a raw JSON string, not a typed struct or double. Its
  `weight` sub-field is polymorphic across the dataset: roughly half the
  records have a bare float (e.g. 1.56), the other half have
  {"v": 1.92, "u": "kg"} - a mid-stream format change by the carrier.
- Only lines that fail to parse as JSON at all are quarantined here - a
  structural problem, not a semantic one. Carriers retry webhooks, so
  repeated (ref, t) pairs across a file are expected raw duplicates and
  are intentionally not deduped in bronze; that happens in silver.
- Idempotency: each load is scoped to a single source file and replaces any
  rows previously loaded from that file (both the data table and its
  quarantine sibling) via an atomic Iceberg overwrite, so reprocessing the
  same file twice never duplicates rows.
"""

import json
from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, LongType, NestedField, StringType, TimestampType

from src.common.catalog import get_catalog
from src.common.ingestion import IngestResult, ingest_jsonl_gz_file, iter_raw_files

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" / "carrier_c"

TABLE_IDENTIFIER = "bronze.carrier_c"
QUARANTINE_TABLE_IDENTIFIER = "bronze.carrier_c_quarantine"

RECORD_SCHEMA = Schema(
    NestedField(1, "ref", StringType(), required=False),
    NestedField(2, "st", IntegerType(), required=False),
    NestedField(3, "t", LongType(), required=False),
    NestedField(4, "dims", StringType(), required=False),
    NestedField(5, "source_ingested_at", StringType(), required=False),
    NestedField(6, "source_file", StringType(), required=True),
    NestedField(7, "source_line_no", IntegerType(), required=True),
    NestedField(8, "_bronze_loaded_at", TimestampType(), required=True),
)


RECORD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=6, field_id=1000, transform=IdentityTransform(), name="source_file")
)


def _row_from_json(obj: dict) -> dict:
    return {
        "ref": obj.get("ref"),
        "st": obj.get("st"),
        "t": obj.get("t"),
        "dims": json.dumps(obj["dims"]) if obj.get("dims") is not None else None,
        "source_ingested_at": obj.get("_ingested_at"),
    }


def ingest_file(catalog: Catalog, path: Path) -> IngestResult:
    return ingest_jsonl_gz_file(
        catalog,
        path,
        TABLE_IDENTIFIER,
        QUARANTINE_TABLE_IDENTIFIER,
        RECORD_SCHEMA,
        RECORD_PARTITION_SPEC,
        _row_from_json,
    )


def ingest_all(raw_dir: Path = RAW_DIR) -> list[IngestResult]:
    catalog = get_catalog()
    return [ingest_file(catalog, path) for path in iter_raw_files(raw_dir, "*.json.gz")]
