"""CLI entrypoint for bronze-layer extraction. Airflow will call the same
per-source ingest_all() functions directly; this script is for manual runs.

Usage:
    python -m src.extract carrier_a
"""

import argparse
import sys

SOURCE_INGESTORS = {
    "carrier_a": "src.bronze.carrier_a",
    "carrier_b": "src.bronze.carrier_b",
    "carrier_c": "src.bronze.carrier_c",
    "parcel_events": "src.bronze.parcel_events",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run bronze-layer extraction for one source.")
    parser.add_argument("source", choices=sorted(SOURCE_INGESTORS))
    parser.add_argument(
        "--force", action="store_true", help="Reprocess every raw file, including ones already ingested."
    )
    args = parser.parse_args()

    module = __import__(SOURCE_INGESTORS[args.source], fromlist=["ingest_all"])
    results = module.ingest_all(force=args.force)

    total_loaded = sum(r.rows_loaded for r in results)
    total_quarantined = sum(r.rows_quarantined for r in results)
    print(f"{args.source}: {len(results)} files, {total_loaded} rows loaded, {total_quarantined} quarantined")
    if total_quarantined:
        for r in results:
            if r.rows_quarantined:
                print(f"  {r.source_file}: {r.rows_quarantined} quarantined")


if __name__ == "__main__":
    sys.exit(main())
