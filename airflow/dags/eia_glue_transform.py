
from datetime import datetime
import time
import boto3

from airflow.sdk import dag, task

REGION = "us-east-1"
GLUE_JOB_NAME = "eia-demand-bronze-to-silver"


@dag(
    dag_id="eia_glue_transform",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["eia", "glue"],
)
def glue_transform():

    @task
    def run_glue():
        client = boto3.client("glue", region_name=REGION)

        response = client.start_job_run(JobName=GLUE_JOB_NAME)
        run_id = response["JobRunId"]

        print(f"Glue job started: {run_id}")

        while True:
            result = client.get_job_run(
                JobName=GLUE_JOB_NAME,
                RunId=run_id,
            )

            status = result["JobRun"]["JobRunState"]
            print(f"Glue status: {status}")

            if status == "SUCCEEDED":
                return

            if status in (
                "FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"
            ):
                raise RuntimeError(f"Glue job failed: {status}")

            time.sleep(15)

    run_glue()


glue_transform()
