"""CLI entrypoint for silver-layer transforms. Airflow will call the same
per-table run() functions directly; this script is for manual runs.

Usage:
    python -m src.transform tracking_events
"""

import argparse
import sys

SOURCE_TRANSFORMS = {
    "tracking_events": "src.silver.tracking_events",
    "parcel_events": "src.silver.parcel_events",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run silver-layer transforms for one table.")
    parser.add_argument("table", choices=sorted(SOURCE_TRANSFORMS))
    args = parser.parse_args()

    module = __import__(SOURCE_TRANSFORMS[args.table], fromlist=["run"])
    result = module.run()

    print(f"{args.table}: {result.rows_loaded} rows loaded, {result.rows_quarantined} quarantined")


if __name__ == "__main__":
    sys.exit(main())
