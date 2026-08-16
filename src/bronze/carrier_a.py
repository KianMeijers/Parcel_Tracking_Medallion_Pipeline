"""Bronze-layer ingestion for carrier_a tracking events.

Source: one gzipped JSON-lines file per day under data/raw/carrier_a/, e.g.
carrier_a_20260501.json.gz. The filename only reflects when the file was
dumped, not when the events inside happened, so it is used purely as a
lineage / idempotency key.

Design choices:
- Fields are kept close to their raw shape (e.g. `ts` stays a string, not a
  parsed timestamp) per the bronze contract: land raw data as-is. Semantic
  validation/normalization (bad timestamps, null tracking numbers, unit
  conversion, etc.) is a silver-layer concern.
- Only lines that fail to parse as JSON at all are quarantined here - a
  structural problem, not a semantic one. Carriers retry webhooks, so
  repeated (tracking_no, status, ts) triples across a file are expected raw
  duplicates and are intentionally *not* deduped in bronze; that happens in
  silver.
- Idempotency: each load is scoped to a single source file and replaces any
  rows previously loaded from that file (both the data table and its
  quarantine sibling) via an atomic Iceberg overwrite, so reprocessing the
  same file twice never duplicates rows.
"""

from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DoubleType, IntegerType, NestedField, StringType, TimestampType

from src.common.catalog import get_catalog
from src.common.ingestion import IngestResult, already_ingested_files, ingest_jsonl_gz_file, iter_raw_files

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" / "carrier_a"

TABLE_IDENTIFIER = "bronze.carrier_a"
QUARANTINE_TABLE_IDENTIFIER = "bronze.carrier_a_quarantine"

RECORD_SCHEMA = Schema(
    NestedField(1, "tracking_no", StringType(), required=False),
    NestedField(2, "status", StringType(), required=False),
    NestedField(3, "ts", StringType(), required=False),
    NestedField(4, "weight_kg", DoubleType(), required=False),
    NestedField(5, "hub", StringType(), required=False),
    NestedField(6, "source_ingested_at", StringType(), required=False),
    NestedField(7, "source_file", StringType(), required=True),
    NestedField(8, "source_line_no", IntegerType(), required=True),
    NestedField(9, "_bronze_loaded_at", TimestampType(), required=True),
)
RECORD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=7, field_id=1000, transform=IdentityTransform(), name="source_file")
)


def _row_from_json(obj: dict) -> dict:
    return {
        "tracking_no": obj.get("tracking_no"),
        "status": obj.get("status"),
        "ts": obj.get("ts"),
        "weight_kg": obj.get("weight_kg"),
        "hub": obj.get("hub"),
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


def ingest_all(raw_dir: Path = RAW_DIR, force: bool = False) -> list[IngestResult]:
    catalog = get_catalog()
    skip = set() if force else already_ingested_files(catalog, TABLE_IDENTIFIER, QUARANTINE_TABLE_IDENTIFIER)
    return [ingest_file(catalog, path) for path in iter_raw_files(raw_dir, "*.json.gz", skip)]
