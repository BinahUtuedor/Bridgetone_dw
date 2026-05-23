from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import time
from dotenv import load_dotenv

load_dotenv()

AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID")
AIRBYTE_API_TOKEN = os.getenv("AIRBYTE_API_TOKEN")
WORK_SPACE_ID = os.getenv("WORK_SPACE_ID")

AIRBYTE_API_URL = "https://api.airbyte.com"

default_args = {
    'owner': 'bridgestone-data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False
}

DBT_PROJECT_DIR = "/usr/local/airflow/dbt_project"
DBT_PROFILES_DIR = "/usr/local/airflow/dbt_profiles"


# ---------------------------
# STEP 1: Trigger Airbyte job
# ---------------------------
def trigger_airbyte_sync(**context):
    url = f"{AIRBYTE_API_URL}/v1/jobs"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {AIRBYTE_API_TOKEN}"
    }

    existing = requests.get(
        f"{AIRBYTE_API_URL}/v1/jobs",
        headers=headers,
        params={"connectionId": AIRBYTE_CONNECTION_ID}
    )

    if existing.status_code == 200:
        jobs = existing.json().get("data", [])
        for job in jobs:
            if job.get("status") in ["running", "pending"]:
                print("Active Airbyte job detected → skipping trigger safely")
                return {"skipped": True}

    payload = {
        "connectionId": AIRBYTE_CONNECTION_ID,
        "jobType": "sync"
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 409:
        print("Airbyte already running (409) → skipping safely")
        return {"skipped": True}

    if response.status_code != 200:
        raise Exception(f"Airbyte trigger failed: {response.text}")

    job = response.json()
    job_id = job.get("jobId")

    if not job_id:
        raise Exception(f"No jobId returned: {job}")

    context["ti"].xcom_push(key="airbyte_job_id", value=job_id)

    print(f"Triggered Airbyte job: {job_id}")
    return job_id


# ---------------------------
# STEP 2: Poll job status
# ---------------------------
def wait_for_airbyte_job(**context):
    ti = context["ti"]
    job_id = ti.xcom_pull(
        key="airbyte_job_id",
        task_ids="trigger_airbyte_sync"
    )

    if not job_id:
        print("No job_id found → exiting safely")
        return "skipped"

    url = f"{AIRBYTE_API_URL}/v1/jobs/{job_id}"

    headers = {
        "Authorization": f"Bearer {AIRBYTE_API_TOKEN}"
    }

    terminal_states = {"succeeded", "failed", "cancelled"}

    print(f"Polling Airbyte job: {job_id}")

    while True:
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Failed to fetch job status: {response.text}")

        job = response.json()
        status = job.get("status")

        print(f"Job status: {status}")

        if status in terminal_states:
            if status != "succeeded":
                raise Exception(f"Airbyte job ended with status: {status}")

            print("Airbyte job succeeded")
            time.sleep(20)
            return status

        time.sleep(30)


# ---------------------------
# DAG
# ---------------------------
with DAG(
    dag_id='bridgestone_daily_pipeline',
    default_args=default_args,
    description='Daily batch pipeline: Airbyte Cloud -> dbt',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['bridgestone', 'analytics']
) as dag:

    trigger_airbyte_sync_task = PythonOperator(
        task_id='trigger_airbyte_sync',
        python_callable=trigger_airbyte_sync
    )

    wait_for_airbyte_task = PythonOperator(
        task_id='wait_for_airbyte_job',
        python_callable=wait_for_airbyte_job
    )

    # ---------------------------
    # DBT PREFLIGHT CLEAN UP TASK
    # ---------------------------
    dbt_clean = BashOperator(
        task_id="dbt_clean",
        bash_command="rm -rf /usr/local/airflow/dbt_project/target /usr/local/airflow/dbt_project/logs"
    )

    # ---------------------------
    # DBT DEBUG (SAFE)
    # ---------------------------
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} && set -e && '
            f'dbt debug --profiles-dir {DBT_PROFILES_DIR} '
            f'|| echo "dbt debug failed (non-blocking)"'
        )
    )

    # ---------------------------
    # DBT DEPS
    # ---------------------------
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="""
        cd /usr/local/airflow/dbt_project &&
        dbt deps --profiles-dir /usr/local/airflow/dbt_profiles
        """
    )

    dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f'cd {DBT_PROJECT_DIR} && set -e && dbt run --select staging --profiles-dir {DBT_PROFILES_DIR}'
    )

    dbt_dimensions = BashOperator(
        task_id='dbt_run_dimensions',
        bash_command=f'cd {DBT_PROJECT_DIR} && set -e && dbt run --select marts.dimensions --profiles-dir {DBT_PROFILES_DIR}'
    )

    dbt_facts = BashOperator(
        task_id='dbt_run_facts',
        bash_command=f'cd {DBT_PROJECT_DIR} && set -e && dbt run --select marts.facts --profiles-dir {DBT_PROFILES_DIR}'
    )

    dbt_reporting = BashOperator(
        task_id='dbt_run_reporting',
        bash_command=f'cd {DBT_PROJECT_DIR} && set -e && dbt run --select reporting --profiles-dir {DBT_PROFILES_DIR}'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && set -e && dbt test --profiles-dir {DBT_PROFILES_DIR}'
    )

    # ---------------------------
    # TASK FLOW (FIXED)
    # ---------------------------
    trigger_airbyte_sync_task >> wait_for_airbyte_task >> dbt_clean >> dbt_debug >> dbt_deps >> dbt_staging >> dbt_dimensions >> dbt_facts >> dbt_reporting >> dbt_test