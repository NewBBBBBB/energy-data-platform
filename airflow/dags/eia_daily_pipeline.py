import os
from datetime import datetime
from pathlib import Path
import subprocess
import time

import boto3
from airflow.sdk import dag, task


REGION = "us-east-1"
GLUE_JOB = "eia-demand-bronze-to-silver"
REDSHIFT_WORKGROUP = "energy-data-wg"
REDSHIFT_DATABASE = "dev"

SQL_DIR = Path("/opt/airflow/sql")
INGESTION_SCRIPT = "/opt/airflow/ingestion/ingestion.py"


def execute_redshift_sql(sql):
    client = boto3.client("redshift-data", region_name=REGION)

    response = client.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        Sql=sql,
    )

    statement_id = response["Id"]

    while True:
        result = client.describe_statement(Id=statement_id)
        status = result["Status"]

        if status == "FINISHED":
            print(f"Redshift SQL succeeded: {statement_id}")
            return

        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(
                f"Redshift SQL failed: {result.get('Error')}"
            )

        time.sleep(3)


@dag(
    dag_id="eia_daily_pipeline",
    schedule="0 10 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["eia", "pipeline"],
)
def eia_daily_pipeline():

    @task
    def ingest_eia():
        subprocess.run(
            [
                "python",
                INGESTION_SCRIPT,
                "--mode",
                "incremental",
            ],
            check=True,
        )

    @task
    def run_glue():
        client = boto3.client("glue", region_name=REGION)

        response = client.start_job_run(JobName=GLUE_JOB)
        run_id = response["JobRunId"]

        print(f"Glue job started: {run_id}")

        while True:
            result = client.get_job_run(
                JobName=GLUE_JOB,
                RunId=run_id,
            )

            job = result["JobRun"]
            status = job["JobRunState"]

            print(f"Glue status: {status}")

            if status == "SUCCEEDED":
                return

            if status in (
                "FAILED", "STOPPED", "TIMEOUT",
                "ERROR", "EXPIRED"
            ):
                raise RuntimeError(
                    f"Glue failed: {job.get('ErrorMessage', status)}"
                )

            time.sleep(15)

    @task
    def load_staging():
        sql = (SQL_DIR / "load_eia_staging.sql").read_text()

        for statement in sql.split(";"):
            if statement.strip():
                execute_redshift_sql(statement.strip())

    @task
    def merge_data():
        sql = (SQL_DIR / "merge_eia_demand.sql").read_text()
        execute_redshift_sql(sql.strip().rstrip(";"))

    @task
    def run_dbt_gold():
        env = os.environ.copy()
        env["DBT_LOG_PATH"] = "/tmp/dbt_logs"
        env["DBT_TARGET_PATH"] = "/tmp/dbt_target"

        subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir",
                "/opt/airflow/dbt/energy_analytics",
                "--profiles-dir",
                "/home/airflow/.dbt",
                "--profile",
                "energy_analytics",
                "--target",
                "dev",
                "--select",
                "stg_eia_demand",
                "fct_hourly_demand",
                "mart_daily_demand",
            ],
            env=env,
            check=True,
        )

    ingest_eia() >> run_glue() >> load_staging() >> merge_data() >> run_dbt_gold()

eia_daily_pipeline()
