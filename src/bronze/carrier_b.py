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

from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DoubleType, IntegerType, NestedField, StringType, StructType, TimestampType

from src.common.catalog import get_catalog
from src.common.ingestion import IngestResult, ingest_jsonl_gz_file, iter_raw_files

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" / "carrier_b"

TABLE_IDENTIFIER = "bronze.carrier_b"
QUARANTINE_TABLE_IDENTIFIER = "bronze.carrier_b_quarantine"

RECORD_SCHEMA = Schema(
    NestedField(1, "barcode", StringType(), required=False),
    NestedField(
        2, "event",
        StructType(
            NestedField(3, "code", IntegerType(), required=False),
            NestedField(4, "desc", StringType(), required=False),
        ),required=False,
    ),
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


def _row_from_json(obj: dict) -> dict:
    return {
        "barcode": obj.get("barcode"),
        "event": obj.get("event"),
        "event_time": obj.get("event_time"),
        "weight_g": obj.get("weight_g"),
        "depot": obj.get("depot"),
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
    return [ingest_file(catalog, path) for path in iter_raw_files(raw_dir, "*.jsonl.gz")]
