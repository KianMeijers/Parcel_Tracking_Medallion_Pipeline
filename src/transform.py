"""CLI entrypoint for silver- and gold-layer transforms. Airflow will call
the same per-table run() functions directly; this script is for manual
runs.

Usage:
    python -m src.transform tracking_events
    python -m src.transform shipments
"""

import argparse
import sys

SOURCE_TRANSFORMS = {
    "tracking_events": "src.silver.tracking_events",
    "parcel_events": "src.silver.parcel_events",
    "dimensions": "src.gold.dimensions",
    "shipments": "src.gold.shipments",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run silver/gold transforms for one table.")
    parser.add_argument("table", choices=sorted(SOURCE_TRANSFORMS))
    args = parser.parse_args()

    module = __import__(SOURCE_TRANSFORMS[args.table], fromlist=["run"])
    result = module.run()

    quarantined = getattr(result, "rows_quarantined", None)
    suffix = f", {quarantined} quarantined" if quarantined is not None else ""
    print(f"{args.table}: {result.rows_loaded} rows loaded{suffix}")


if __name__ == "__main__":
    sys.exit(main())
