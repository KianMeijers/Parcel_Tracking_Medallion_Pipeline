"""Data-quality gates run between medallion layers.

Each check queries currently-committed tables and raises DataQualityError
if a threshold is breached. Callers - the DAG's @task wrappers - let that
exception propagate, which fails the Airflow task and blocks whatever the
DAG has wired downstream of it.
"""

from dataclasses import dataclass

from pyiceberg.catalog import Catalog

from src.common.catalog import get_catalog

QUARANTINE_RATE_THRESHOLD = 0.05

BRONZE_TABLES = (
    ("bronze.carrier_a", "bronze.carrier_a_quarantine"),
    ("bronze.carrier_b", "bronze.carrier_b_quarantine"),
    ("bronze.carrier_c", "bronze.carrier_c_quarantine"),
    ("bronze.parcel_events", "bronze.parcel_events_quarantine"),
)

SILVER_TABLES = (
    ("silver.tracking_events", "silver.tracking_events_quarantine"),
    ("silver.parcel_events", "silver.parcel_events_quarantine"),
)

GOLD_SHIPMENTS_TABLE = "gold.shipments"


class DataQualityError(Exception):
    """Raised when a data-quality gate fails."""


@dataclass
class QuarantineRateResult:
    table: str
    rows_loaded: int
    rows_quarantined: int
    rate: float


def _table_row_count(catalog: Catalog, identifier: str) -> int:
    if not catalog.table_exists(identifier):
        return 0
    return catalog.load_table(identifier).scan().to_arrow().num_rows


def _quarantine_rate(catalog: Catalog, table_identifier: str, quarantine_identifier: str) -> QuarantineRateResult:
    rows_loaded = _table_row_count(catalog, table_identifier)
    rows_quarantined = _table_row_count(catalog, quarantine_identifier)
    total = rows_loaded + rows_quarantined
    rate = rows_quarantined / total if total else 0.0
    return QuarantineRateResult(table_identifier, rows_loaded, rows_quarantined, rate)


def _enforce_quarantine_rates(results: list[QuarantineRateResult]) -> list[QuarantineRateResult]:
    breached = [r for r in results if r.rate > QUARANTINE_RATE_THRESHOLD]
    if breached:
        detail = ", ".join(
            f"{r.table} at {r.rate:.1%} ({r.rows_quarantined}/{r.rows_loaded + r.rows_quarantined} rows)"
            for r in breached
        )
        raise DataQualityError(f"quarantine rate exceeds {QUARANTINE_RATE_THRESHOLD:.0%} threshold: {detail}")
    return results


def check_bronze_quarantine_rates(catalog: Catalog) -> list[QuarantineRateResult]:
    return _enforce_quarantine_rates([_quarantine_rate(catalog, table, quarantine) for table, quarantine in BRONZE_TABLES])


def check_silver_quarantine_rates(catalog: Catalog) -> list[QuarantineRateResult]:
    return _enforce_quarantine_rates([_quarantine_rate(catalog, table, quarantine) for table, quarantine in SILVER_TABLES])


def check_gold_shipments(catalog: Catalog) -> dict:
    table = catalog.load_table(GOLD_SHIPMENTS_TABLE)
    rows = table.scan().to_arrow().to_pylist()

    problems: list[str] = []
    if not rows:
        problems.append("table is empty")

    parcel_ids = [r["parcel_id"] for r in rows]
    duplicate_count = len(parcel_ids) - len(set(parcel_ids))
    if duplicate_count:
        problems.append(f"{duplicate_count} duplicate parcel_id rows")

    negative_transit = [r["parcel_id"] for r in rows if r["transit_hours"] is not None and r["transit_hours"] < 0]
    if negative_transit:
        problems.append(
            f"{len(negative_transit)} rows with negative transit_hours (delivered before accepted), "
            f"e.g. parcel_id={negative_transit[0]}"
        )

    contradictory_terminal = [
        r["parcel_id"] for r in rows if r["is_delivered"] and (r.get("is_lost") or r.get("is_returned"))
    ]
    if contradictory_terminal:
        problems.append(
            f"{len(contradictory_terminal)} rows flagged both delivered and lost/returned, "
            f"e.g. parcel_id={contradictory_terminal[0]}"
        )

    if problems:
        raise DataQualityError(f"{GOLD_SHIPMENTS_TABLE}: " + "; ".join(problems))

    return {"row_count": len(rows)}


def run_bronze_gate() -> list[QuarantineRateResult]:
    return check_bronze_quarantine_rates(get_catalog())


def run_silver_gate() -> list[QuarantineRateResult]:
    return check_silver_quarantine_rates(get_catalog())


def run_gold_gate() -> dict:
    return check_gold_shipments(get_catalog())
