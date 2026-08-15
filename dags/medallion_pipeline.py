"""Airflow DAG orchestrating the bronze -> silver -> gold medallion pipeline.

Sequencing and retries only - no data crosses task boundaries via XCom. Each
task calls into src/{bronze,silver,gold}, which reads its inputs from and
writes its outputs to Iceberg tables directly, so the tables themselves are
the hand-off between stages.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pendulum

from airflow.sdk import dag, task, task_group


@dag(
    dag_id="medallion_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["bronze", "silver", "gold"],
)
def medallion_pipeline():
    @task_group(group_id="bronze")
    def bronze():
        @task
        def carrier_a():
            from src.bronze.carrier_a import ingest_all

            ingest_all()

        @task
        def carrier_b():
            from src.bronze.carrier_b import ingest_all

            ingest_all()

        @task
        def carrier_c():
            from src.bronze.carrier_c import ingest_all

            ingest_all()

        @task
        def parcel_events():
            from src.bronze.parcel_events import ingest_all

            ingest_all()

        return carrier_a(), carrier_b(), carrier_c(), parcel_events()

    @task_group(group_id="silver")
    def silver(carrier_a_t, carrier_b_t, carrier_c_t, bronze_parcel_events_t):
        @task
        def tracking_events():
            from src.silver.tracking_events import run

            run()

        @task
        def parcel_events():
            from src.silver.parcel_events import run

            run()

        tracking_events_t = tracking_events()
        parcel_events_t = parcel_events()

        [carrier_a_t, carrier_b_t, carrier_c_t] >> tracking_events_t
        bronze_parcel_events_t >> parcel_events_t

        return tracking_events_t, parcel_events_t

    @task_group(group_id="gold")
    def gold(tracking_events_t, silver_parcel_events_t):
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
        [tracking_events_t, silver_parcel_events_t] >> shipments_t

    bronze_carrier_a, bronze_carrier_b, bronze_carrier_c, bronze_parcel_events = bronze()
    silver_tracking_events, silver_parcel_events = silver(
        bronze_carrier_a, bronze_carrier_b, bronze_carrier_c, bronze_parcel_events
    )
    gold(silver_tracking_events, silver_parcel_events)


medallion_pipeline()
