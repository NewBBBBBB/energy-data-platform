from datetime import datetime, timedelta
from airflow.sdk import dag, task, Param

@dag(
    dag_id="eia_demand_ingestion",
    schedule=None,
    start_date=datetime(2026, 10, 30),
    catchup=False,
    params={
        "start_datetime": Param(
            "2025-01-01T00:00:00Z",
            type="string",
            format="date-time",
            title="Start Date & Time (UTC)",
        ),
        "end_datetime": Param(
            "2025-01-03T23:00:00Z",
            type="string",
            format="date-time",
            title="End Date & Time (UTC)",
        ),
    },
    tags=["eia", "demand"]
)
def eia_demand_ingestion():

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def ingest_demand(**context):
        import subprocess

        params = context["params"]

        subprocess.run(
            [
                "python",
                "/opt/airflow/ingestion/ingestion.py",
                "--start-time", params["start_datetime"],
                "--end-time", params["end_datetime"],
            ],
            check=True,
        )

    ingest_demand()

eia_demand_ingestion()
