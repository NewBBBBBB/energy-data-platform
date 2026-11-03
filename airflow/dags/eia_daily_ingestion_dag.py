from datetime import datetime, timedelta, timezone
from airflow.sdk import dag, task


@dag(
    dag_id="eia_daily_incremental_ingestion",
    start_date=datetime(2026, 10, 30, tzinfo=timezone.utc),
    schedule="0 10 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["eia", "demand", "incremental"],
)
def eia_daily_incremental_ingestion():

    @task(
        retries=2,
        retry_delay=timedelta(minutes=5)
    )
    def ingest_incremental():
        import subprocess

        subprocess.run(
            [
                "python",
                "/opt/airflow/ingestion/ingestion.py",
                "--mode",
                "incremental",
            ],
            check=True,
        )

    ingest_incremental()


eia_daily_incremental_ingestion()
