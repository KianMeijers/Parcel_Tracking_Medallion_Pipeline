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

The read/parse/quarantine/overwrite mechanics are shared across all bronze
sources - see src/common/ingestion.py. This module only defines the schema
and the raw-JSON-to-row mapping.
"""

from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, NestedField, StringType, TimestampType

from src.common.catalog import get_catalog
from src.common.ingestion import IngestResult, ingest_jsonl_gz_file, iter_raw_files

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


def _row_from_json(obj: dict) -> dict:
    return {
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
