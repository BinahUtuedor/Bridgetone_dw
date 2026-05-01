from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import time
import os


# =========================
# CONFIG (UPDATE THESE)
# =========================
AIRBYTE_API_TOKEN = os.getenv("AIRBYTE_API_TOKEN")
AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID")

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/.dbt"

# =========================
# DEFAULT DAG CONFIG
# =========================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# =========================
# FUNCTION: TRIGGER AIRBYTE SYNC
# =========================
def trigger_airbyte_sync(**kwargs):
    url = "https://api.airbyte.com/v1/jobs"

    headers = {
        "Authorization": f"Bearer {AIRBYTE_API_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "connectionId": AIRBYTE_CONNECTION_ID
    }

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()

    job_id = response.json()["jobId"]

    print(f"Triggered Airbyte Job: {job_id}")

    return job_id


# =========================
# FUNCTION: WAIT FOR AIRBYTE
# =========================
def wait_for_airbyte_job(**kwargs):
    ti = kwargs['ti']
    job_id = ti.xcom_pull(task_ids='trigger_airbyte_sync')

    url = f"https://api.airbyte.com/v1/jobs/{job_id}"

    headers = {
        "Authorization": f"Bearer {AIRBYTE_API_TOKEN}"
    }

    while True:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        status = response.json()["status"]
        print(f"Airbyte job status: {status}")

        if status == "succeeded":
            break
        elif status in ["failed", "cancelled"]:
            raise Exception(f"Airbyte job failed with status: {status}")

        time.sleep(30)


# =========================
# DAG DEFINITION
# =========================
with DAG(
    dag_id="bridgestone_pipeline",
    default_args=default_args,
    description="Airbyte → dbt → Snowflake pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bridgestone", "elt"],
) as dag:

    # 1. Trigger Airbyte
    trigger_sync = PythonOperator(
        task_id="trigger_airbyte_sync",
        python_callable=trigger_airbyte_sync,
    )

    # 2. Wait for completion
    wait_sync = PythonOperator(
        task_id="wait_for_airbyte_job",
        python_callable=wait_for_airbyte_job,
    )

    # 3. Run dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --profiles-dir {DBT_PROFILES_DIR}
        """
    )

    # 4. Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt test --profiles-dir {DBT_PROFILES_DIR}
        """
    )

    # =========================
    # PIPELINE ORDER
    # =========================
    trigger_sync >> wait_sync >> dbt_run >> dbt_test