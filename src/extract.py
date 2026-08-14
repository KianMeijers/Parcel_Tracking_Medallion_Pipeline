"""CLI entrypoint for bronze-layer extraction. Airflow will call the same
per-carrier ingest_all() functions directly; this script is for manual runs.

Usage:
    python -m src.extract carrier_a
"""

import argparse
import sys

CARRIER_INGESTORS = {
    "carrier_a": "src.bronze.carrier_a",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run bronze-layer extraction for one carrier.")
    parser.add_argument("carrier", choices=sorted(CARRIER_INGESTORS))
    args = parser.parse_args()

    module = __import__(CARRIER_INGESTORS[args.carrier], fromlist=["ingest_all"])
    results = module.ingest_all()

    total_loaded = sum(r.rows_loaded for r in results)
    total_quarantined = sum(r.rows_quarantined for r in results)
    print(f"{args.carrier}: {len(results)} files, {total_loaded} rows loaded, {total_quarantined} quarantined")
    if total_quarantined:
        for r in results:
            if r.rows_quarantined:
                print(f"  {r.source_file}: {r.rows_quarantined} quarantined")


if __name__ == "__main__":
    sys.exit(main())
