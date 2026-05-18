from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import time
import os

# =========================
# CONFIG
# =========================
AIRBYTE_API_TOKEN = os.getenv("AIRBYTE_API_TOKEN")
AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID")

DBT_PROJECT_DIR = "/usr/local/airflow/dbt_project"
DBT_PROFILES_DIR = "/usr/local/airflow/.dbt"

# =========================
# DEFAULT ARGS (kept same style as yours)
# =========================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

# =========================
# AIRBYTE TRIGGER
# =========================
def trigger_airbyte_sync(**kwargs):
    url = "https://api.airbyte.com/v1/jobs"

    headers = {
        "Authorization": f"Bearer {AIRBYTE_API_TOKEN}",
        "Content-Type": "application/json",
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
# AIRBYTE POLLING
# =========================
def wait_for_airbyte_job(**kwargs):
    ti = kwargs["ti"]
    job_id = ti.xcom_pull(task_ids="trigger_airbyte_sync")

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
            print("Airbyte sync completed successfully")
            break

        if status in ["failed", "cancelled"]:
            raise Exception(f"Airbyte job failed: {status}")

        time.sleep(30)


# =========================
# DAG
# =========================
with DAG(
    dag_id="bridgestone_pipeline",
    default_args=default_args,
    description="Airbyte → Snowflake → dbt ELT pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bridgestone", "elt", "airbyte", "dbt"],
) as dag:

    # -------------------------
    # 1. Trigger Airbyte Sync
    # -------------------------
    trigger_sync = PythonOperator(
        task_id="trigger_airbyte_sync",
        python_callable=trigger_airbyte_sync,
    )

    # -------------------------
    # 2. Wait for completion
    # -------------------------
    wait_sync = PythonOperator(
        task_id="wait_for_airbyte_job",
        python_callable=wait_for_airbyte_job,
    )

    # -------------------------
    # 3. dbt STAGING layer
    # -------------------------
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --select staging --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # -------------------------
    # 4. dbt DIMENSIONS
    # -------------------------
    dbt_dimensions = BashOperator(
        task_id="dbt_run_dimensions",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --select marts.dimensions --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # -------------------------
    # 5. dbt FACTS
    # -------------------------
    dbt_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --select marts.facts --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # -------------------------
    # 6. dbt REPORTING layer
    # -------------------------
    dbt_reporting = BashOperator(
        task_id="dbt_run_reporting",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --select reporting --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # -------------------------
    # 7. dbt TESTS (data quality gate)
    # -------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt test --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # =========================
    # PIPELINE FLOW
    # =========================
    trigger_sync >> wait_sync >> dbt_staging >> dbt_dimensions >> dbt_facts >> dbt_reporting >> dbt_test