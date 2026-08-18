"""Airflow DAG orchestrating the bronze -> silver -> gold medallion pipeline.

Sequencing and retries only - no data crosses task boundaries via XCom. Each
task calls into src/{bronze,silver,gold}, which reads its inputs from and
writes its outputs to Iceberg tables directly, so the tables themselves are
the hand-off between stages.

A data-quality gate task sits after each layer (src/quality.py). Each gate
queries the tables that layer just wrote and raises if an anomaly threshold
is breached; that failure fails the gate task, which blocks every
downstream task so a bad batch never reaches the next layer.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pendulum

from airflow.sdk import Param, dag, get_current_context, task, task_group


@dag(
    dag_id="medallion_pipeline",
    schedule=None, # Set @daily for daily updates
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["bronze", "silver", "gold"],
    params={
        "force_bronze": Param(
            False,
            type="boolean",
            description=(
                "Reprocess every raw bronze file, including ones already ingested, "
                "instead of skipping files already committed to bronze."
            ),
        )
    },
)
def medallion_pipeline():
    @task_group(group_id="bronze")
    def bronze():
        @task
        def carrier_a():
            from src.bronze.carrier_a import ingest_all

            ingest_all(force=get_current_context()["params"]["force_bronze"])

        @task
        def carrier_b():
            from src.bronze.carrier_b import ingest_all

            ingest_all(force=get_current_context()["params"]["force_bronze"])

        @task
        def carrier_c():
            from src.bronze.carrier_c import ingest_all

            ingest_all(force=get_current_context()["params"]["force_bronze"])

        @task
        def parcel_events():
            from src.bronze.parcel_events import ingest_all

            ingest_all(force=get_current_context()["params"]["force_bronze"])

        return carrier_a(), carrier_b(), carrier_c(), parcel_events()

    @task
    def bronze_quality_gate():
        from src.quality import run_bronze_gate

        run_bronze_gate()

    @task_group(group_id="silver")
    def silver():
        @task
        def tracking_events():
            from src.silver.tracking_events import run

            run()

        @task
        def parcel_events():
            from src.silver.parcel_events import run

            run()

        return tracking_events(), parcel_events()

    @task
    def silver_quality_gate():
        from src.quality import run_silver_gate

        run_silver_gate()

    @task_group(group_id="gold")
    def gold():
        @task
        def dimensions():
            from src.gold.dimensions import run

            run()

        @task
        def shipments():
            from src.gold.shipments import run

            run()

        dimensions_t = dimensions()
        shipments_t = shipments()
        dimensions_t >> shipments_t
        return shipments_t

    @task
    def gold_quality_gate():
        from src.quality import run_gold_gate

        run_gold_gate()

    bronze_tasks = list(bronze())
    bronze_gate = bronze_quality_gate()
    bronze_tasks >> bronze_gate

    silver_tasks = list(silver())
    bronze_gate >> silver_tasks
    silver_gate = silver_quality_gate()
    silver_tasks >> silver_gate

    gold_shipments = gold()
    silver_gate >> gold_shipments
    gold_shipments >> gold_quality_gate()


medallion_pipeline()
