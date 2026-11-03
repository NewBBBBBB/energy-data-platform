
from datetime import datetime
from pathlib import Path
import time

import boto3
from airflow.sdk import dag, task

REGION = "us-east-1"
WORKGROUP = "energy-data-wg"
DATABASE = "dev"
SQL_DIR = Path("/opt/airflow/sql")


def execute_sql(sql):
    client = boto3.client("redshift-data", region_name=REGION)

    response = client.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sql=sql,
    )

    statement_id = response["Id"]

    while True:
        result = client.describe_statement(Id=statement_id)
        status = result["Status"]

        if status == "FINISHED":
            print(f"SQL completed: {statement_id}")
            return

        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(
                f"Redshift SQL failed: {result.get('Error')}"
            )

        time.sleep(3)


@dag(
    dag_id="eia_redshift_loader",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["eia", "redshift"],
)
def redshift_loader():

    @task
    def load_staging():
        sql = (SQL_DIR / "load_eia_staging.sql").read_text()

        # Execute TRUNCATE and COPY sequentially
        statements = [
            statement.strip()
            for statement in sql.split(";")
            if statement.strip()
        ]

        for statement in statements:
            execute_sql(statement)

    @task
    def merge_data():
        sql = (SQL_DIR / "merge_eia_demand.sql").read_text()
        execute_sql(sql.strip().rstrip(";"))

    load_staging() >> merge_data()


redshift_loader()
