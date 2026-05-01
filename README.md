# BRIDGESTONE DATA WAREHOUSE PROJECT
## Overview
This repository contains an end‑to‑end ELT data pipeline that ingests raw CSV files from Google Drive, stages and transforms them using dbt, orchestrates workflows with Apache Airflow, and loads curated data into Snowflake.
The final data model follows a star schema optimised for analytics and reporting in Power BI.

## Technology Stack

| Component | Technology |
| --- | --- |
| Ingestion | Airbyte (Cloud) |
| Data Warehouse | Snowflake |
| Transformation & Testing | dbt |
| Orchestration | Apache Airflow |
| Containerisation | Docker |
| BI & Reporting | Power BI |

## End‑to‑End Workflow
Ingestion – Airbyte extracts CSV files from Google Drive and loads them into Snowflake’s RAW schema.

Staging – dbt standardises and cleans raw data into staging views.

Transformation – dbt builds intermediate, dimension, and fact models following a star schema.

Testing – dbt applies schema and custom tests to ensure data quality.

Orchestration – Airflow schedules and manages the ELT workflow.

Documentation – dbt generates lineage graphs, documentation, and an ERD.


## 📊 Architecture Diagrams
Below are two versions of the architecture diagram — Mermaid and ASCII.

### Mermaid Architecture Diagram
```text
flowchart LR
    subgraph Source
        A[Google Drive<br/>CSV Files]
    end

    subgraph Ingestion
        B[Airbyte Cloud<br/>Ingestion]
    end

    subgraph Warehouse[Snowflake]
        C[RAW Schema]
        D[STAGING Schema]
        E[INTERMEDIATE Schema]
        F[MARTS Schema<br/>Dimensions & Facts]
    end

    subgraph Transform[dbt]
        G[Staging Models]
        H[Intermediate Models]
        I[Dim & Fact Models]
        J[dbt Tests]
    end

    subgraph Orchestration[Apache Airflow]
        K[Airbyte Sync Trigger]
        L[dbt Run]
        M[dbt Test]
    end

    subgraph BI
        N[Power BI Reports]
    end

    A --> B --> C
    C --> G --> D
    D --> H --> E
    E --> I --> F
    I --> N

    K --> B
    L --> I
    M --> J
```

### ASCII Architecture Diagram

```text
+------------------+        +------------------+        +----------------------+
|  Google Drive    | -----> |     Airbyte      | -----> |   Snowflake RAW      |
|  (CSV Files)     |        |   (Ingestion)    |        |      Schema          |
+------------------+        +------------------+        +----------------------+
                                                           |
                                                           v
                                                +----------------------+
                                                |  STAGING Schema      |
                                                |  (dbt Staging Models)|
                                                +----------------------+
                                                           |
                                                           v
                                                +----------------------+
                                                | INTERMEDIATE Schema  |
                                                | (dbt Transformations)|
                                                +----------------------+
                                                           |
                                                           v
                                                +----------------------+
                                                |   MARTS Schema       |
                                                | (Dims & Fact Tables) |
                                                +----------------------+
                                                           |
                                                           v
                                                +----------------------+
                                                |     Power BI         |
                                                |   Analytics Layer    |
                                                +----------------------+

+------------------+        +------------------+        +----------------------+
| Apache Airflow   | -----> | Trigger Airbyte  | -----> | Run dbt Models       |
| (Orchestration)  |        | Run dbt / Tests  |        | Validate & Publish   |
+------------------+        +------------------+        +----------------------+
```

## Project Structure

```text
bridgestone_dw/
├── airflow/
│   ├── dags/
│   │   └── bridgestone_pipeline.py      # Airflow DAG
│   ├── plugins/
│   ├── logs/
│   └── docker-compose.yml
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   └── stores/
│   │   │       ├── stg_store_files.sql
│   │   │       └── schema.yml
│   │   ├── intermediate/
│   │   │   └── int_store_unioned.sql
│   │   └── marts/
│   │       ├── dimensions/
│   │       │   ├── dim_stores.sql
│   │       │   ├── dim_products.sql
│   │       │   ├── dim_customers.sql
│   │       │   └── dim_date.sql
│   │       └── facts/
│   │           └── fct_store_sales.sql
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── tests/
│   ├── dbt_project.yml
│   └── README.md
├── venv/
├── .env
├── .gitignore
├── README.md
└── requirements.txt
```

## Setup Instructions
### 1. Prerequisites
Ensure the following are installed or available:

Docker Desktop

Python 3.8+

Snowflake account

Airbyte Cloud account

2. Clone Repository & Create Virtual Environment

git clone <repo-url>
cd bridgestone_dw

Create a virtual environment

```bash
python -m venv venv
```
Activate our virtual environment by running

#### Windows: 
```bash
venv\Scripts\activate
```
#### macOS/Linux: 
```bash
source venv/bin/activate
```
create a requirements.txt file in the root of your project directory

pip install -r requirements.txt


### 3. Snowflake Configuration
Run the following SQL as ACCOUNTADMIN:

-- Warehouse, database, schemas

```sql
CREATE WAREHOUSE IF NOT EXISTS BRIDGESTONE_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS BRIDGESTONE_DW;
USE DATABASE BRIDGESTONE_DW;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS MARTS;

-- dbt role and user
CREATE ROLE IF NOT EXISTS DBT_ROLE;
GRANT USAGE ON WAREHOUSE BRIDGESTONE_WH TO ROLE DBT_ROLE;
GRANT ALL ON DATABASE BRIDGESTONE_DW TO ROLE DBT_ROLE;

CREATE USER IF NOT EXISTS DBT_USER
  PASSWORD = 'StrongPass123!'
  DEFAULT_ROLE = DBT_ROLE
  DEFAULT_WAREHOUSE = BRIDGESTONE_WH;

GRANT ROLE DBT_ROLE TO USER DBT_USER;


Create a .env file:
```
```text
SNOWFLAKE_ACCOUNT=<your_account>
SNOWFLAKE_USER=<your_username>
SNOWFLAKE_PASSWORD=<your_password>
SNOWFLAKE_ROLE=DBT_ROLE
SNOWFLAKE_WAREHOUSE=BRIDGESTONE_WH
SNOWFLAKE_DATABASE=BRIDGESTONE_DW
SNOWFLAKE_SCHEMA=STAGING
```

### 4. Airbyte Configuration
In Airbyte Cloud:

Source: Google Drive
Authenticate

Select folder: /raw_csv/

Glob pattern: data_2/*.csv

Destination: Snowflake
Database: BRIDGESTONE_DW

Schema: RAW

Table: STORE_FILES_RAW

Sync Mode: Append

Frequency: Daily

### 5. dbt Setup
Install dbt:


pip install dbt-snowflake
dbt init dbt_project


Update dbt_project.yml:

models:
  dbt_project:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: view
      +schema: intermediate
    marts:
      +materialized: table
      +schema: marts


Macro to prevent schema prefixing:

-- macros/generate_schema_name.sql
```c
{% macro generate_schema_name(custom_schema_name, node) %}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{% endmacro %}
```

Define source:

version: 2
sources:
  - name: raw
    database: BRIDGESTONE_DW
    schema: RAW
    tables:
      - name: STORE_FILES_RAW


Run dbt:

```bash
dbt debug
dbt run
dbt test
dbt docs generate
dbt docs serve
```

### 6. Airflow Orchestration

🛠️ Airflow Configuration (Full DAG Included)
This project uses Apache Airflow to orchestrate:

📄 Airflow DAG: bridgestone_pipeline.py

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import time
import os

## =========================
## CONFIG (UPDATE THESE)
## =========================
AIRBYTE_API_TOKEN = os.getenv("AIRBYTE_API_TOKEN")
AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID")

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/.dbt"

## =========================
## DEFAULT DAG CONFIG
## =========================
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

## =========================
## FUNCTION: WAIT FOR AIRBYTE
## =========================
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

## =========================
## DAG DEFINITION
## =========================
with DAG(
    dag_id="bridgestone_pipeline",
    default_args=default_args,
    description="Airbyte → dbt → Snowflake pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bridgestone", "elt"],
) as dag:

    trigger_sync = PythonOperator(
        task_id="trigger_airbyte_sync",
        python_callable=trigger_airbyte_sync,
    )

    wait_sync = PythonOperator(
        task_id="wait_for_airbyte_job",
        python_callable=wait_for_airbyte_job,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --profiles-dir {DBT_PROFILES_DIR}
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt test --profiles-dir {DBT_PROFILES_DIR}
        """
    )

    trigger_sync >> wait_sync >> dbt_run >> dbt_test
```

Airbyte sync

dbt run

dbt test

Your full DAG is included below.
Airbyte Credentials
API Token: Airbyte Cloud → Settings → Create Application

Connection ID: Airbyte → Connections → URL parameter

Environment Variables

AIRBYTE_API_TOKEN=<your_token>
AIRBYTE_CONNECTION_ID=<your_connection_id>


### Start Airflow
cd airflow and run

```bash
docker compose up -d
```
Access UI: http://localhost:8080 (admin/admin)

### Run the Pipeline
Enable DAG: bridgestone_pipeline

Trigger DAG

### Pipeline flow:
Airbyte Sync → Wait for Completion → dbt run → dbt test


### Data Models
Staging (STAGING)
stg_store_files – cleaned and standardised raw data.

### Intermediate (INTERMEDIATE)
int_store_unioned – enriched with calculated fields (e.g., derived amounts, date parts).

### Marts (MARTS)
Dimensions:

dim_stores

dim_products

dim_customers

dim_date

Fact:

fct_store_sales – transaction-level fact table with foreign keys to all dimensions.

```text
erDiagram
    FACT_STORE_SALES {
        int sale_id PK
        int store_id FK
        int product_id FK
        int customer_id FK
        date sale_date
        float amount
    }

    DIM_STORES {
        int store_id PK
        string store_name
        string region
    }

    DIM_PRODUCTS {
        int product_id PK
        string product_name
        string category
    }

    DIM_CUSTOMERS {
        int customer_id PK
        string customer_name
        string segment
    }

    DIM_DATE {
        date date_key PK
        int year
        int month
        int day
    }

    FACT_STORE_SALES ||--|| DIM_STORES : store_id
    FACT_STORE_SALES ||--|| DIM_PRODUCTS : product_id
    FACT_STORE_SALES ||--|| DIM_CUSTOMERS : customer_id
    FACT_STORE_SALES ||--|| DIM_DATE : sale_date
```

### Testing
dbt tests include:

Schema tests: unique, not_null

Custom tests: business logic validations

Source freshness checks

Run tests:

dbt test

## 📈 Future Enhancements
Add CI/CD (GitHub Actions)

Integrate data quality monitoring (Great Expectations / Elementary)

Add Slack or email alerting

Optimise Snowflake compute usage

## 👤 Author
Binah Utuedor  
Lead Data Architect & Senior Data Engineer