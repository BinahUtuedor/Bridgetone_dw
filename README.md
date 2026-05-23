# BRIDGESTONE DATA WAREHOUSE PROJECT

## Overview

This repository contains an end-to-end ELT data pipeline that ingests raw CSV files from Google Drive, stages and transforms them using dbt, orchestrates workflows with Apache Airflow (Astro), and loads curated data into Snowflake. The final data model follows a star schema optimized for analytics and reporting in Power BI.

## Technology Stack

| Component | Technology |
| --- | --- |
| Ingestion | Airbyte (Cloud) |
| Data Warehouse | Snowflake |
| Transformation & Testing | dbt Core |
| Orchestration | Apache Airflow (Astro CLI) |
| Containerization | Docker |
| BI & Reporting | Power BI |
| Version Control | Git |

## End-to-End Workflow

```text
Ingestion → Airbyte extracts CSV files from Google Drive → Snowflake RAW schema
↓
Staging → dbt standardizes and cleans raw data → STAGING schema (views)
↓
Transformation → dbt builds intermediate models → INTERMEDIATE schema (views)
↓
Mart Construction → dbt builds dimensions and facts → MARTS schema (tables)
↓
Reporting → dbt builds aggregated reports → MARTS schema (tables)
↓
Testing → dbt applies schema + custom tests → Quality validation
↓
Orchestration → Airflow schedules the entire workflow daily
↓
Documentation → dbt docs generate lineage + ERD + metadata
↓
Visualization → Power BI consumes curated tables for dashboards
```
## Architecture Diagram

```text
+------------------+        +------------------+        +----------------------+
|  Google Drive    | -----> |     Airbyte      | -----> |   Snowflake RAW      |
|  (CSV Files)     |        |   (Ingestion)    |        |      Schema          |
+------------------+        +------------------+        | STORE_FILES_RAW      |
                                                        +----------------------+
                                                           |
                                                           v
+------------------+        +------------------+        +----------------------+
|  Apache Airflow  | -----> |   dbt run        | -----> |   STAGING Schema     |
|  (Orchestration) |        |   dbt test       |        | stg_store_files      |
|                  |        |   dbt docs       |        | (views)              |
+------------------+        +------------------+        +----------------------+
                                                           |
                                                           v
                                                        +----------------------+
                                                        | INTERMEDIATE Schema  |
                                                        | int_store_unioned    |
                                                        | (views)              |
                                                        +----------------------+
                                                           |
                                                           v
                                                        +----------------------+
                                                        |   MARTS Schema       |
                                                        | Dimensions:          |
                                                        | - dim_stores         |
                                                        | - dim_products       |
                                                        | - dim_customers      |
                                                        | - dim_date           |
                                                        |                      |
                                                        | Facts:               |
                                                        | - fct_store_sales    |
                                                        |                      |
                                                        | Reporting:           |
                                                        | - rpt_sales_summary  |
                                                        | - rpt_monthly_store  |
                                                        | - rpt_product_perf   |
                                                        | - rpt_customer_sales |
                                                        +----------------------+
                                                           |
                                                           v
                                                        +----------------------+
                                                        |     Power BI         |
                                                        |   Dashboards         |
                                                        |   & Analytics        |
                                                        +----------------------+
```

## ER Diagram
```text
erDiagram
    DIM_DATE ||--o{ FCT_STORE_SALES : "date_key"
    DIM_STORES ||--o{ FCT_STORE_SALES : "store_id"
    DIM_PRODUCTS ||--o{ FCT_STORE_SALES : "product_id"
    DIM_CUSTOMERS ||--o{ FCT_STORE_SALES : "customer_id"

    DIM_DATE {
        number date_key PK
        date date
        int year
        int month
        int day
        int quarter
        string month_name
        string day_name
        int day_of_week
        boolean is_weekend
        string quarter_name
        string year_month
        number year_month_key
        boolean is_today
        boolean is_current_year
    }

    DIM_STORES {
        string store_id PK
    }

    DIM_PRODUCTS {
        string product_id PK
        string product_name
        string category
    }

    DIM_CUSTOMERS {
        string customer_id PK
    }

    FCT_STORE_SALES {
        string transaction_id PK
        string store_id FK
        string customer_id FK
        string product_id FK
        timestamp transaction_timestamp
        date transaction_date
        int quantity
        float unit_price
        float total_amount
        number date_key FK
    }

    RPT_SALES_SUMMARY {
        date date PK
        float total_sales
        int total_quantity
        int total_transactions
        int unique_customers
    }

    RPT_MONTHLY_STORE_SALES {
        int year PK
        int month PK
        string store_id PK
        float monthly_sales
        int items_sold
        int transactions
    }

    RPT_PRODUCT_PERFORMANCE {
        int year PK
        int month PK
        string category PK
        string product_name PK
        int units_sold
        float revenue
    }

    RPT_CUSTOMER_SALES {
        string customer_id PK
        int transaction_count
        float lifetime_value
        float avg_transaction_value
    }
```

## Project Structure
```text
bridgestone_dw/
│
├── .astro/
│   ├── config.yaml
│   ├── config.yaml.lock
│   ├── dag_integrity_exceptions.txt
│   └── test_dag_integrity_default.py
│
├── dags/
│   ├── __pycache__/
│   ├── .airflowignore
│   └── bridgestone_pipeline.py      # Airflow DAG definition
│
├── dbt_project/
│   ├── analyses/
│   │   └── .gitkeep
│   ├── macros/
│   │   ├── generate_schema_name.sql
│   │   ├── surrogate_keys.sql
│   │   ├── auditing.sql
│   │   ├── formatting.sql
│   │   ├── dates.sql
│   │   ├── testing.sql
│   │   ├── incremental.sql
│   │   ├── null_handling.sql
│   │   ├── kpi_calculations.sql
│   │   └── utilities.sql
│   ├── models/
│   │   ├── staging/
│   │   │   └── stores/
│   │   │       ├── stg_store_files.sql
│   │   │       └── schema.yml
│   │   ├── intermediate/
│   │   │   └── int_store_unioned.sql
│   │   ├── marts/
│   │   │   ├── dimensions/
│   │   │   │   ├── dim_stores.sql
│   │   │   │   ├── dim_products.sql
│   │   │   │   ├── dim_customers.sql
│   │   │   │   └── dim_date.sql
│   │   │   ├── facts/
│   │   │   │   └── fct_store_sales.sql
│   │   │   └── reporting/
│   │   │       ├── rpt_sales_summary.sql
│   │   │       ├── rpt_monthly_store_sales.sql
│   │   │       ├── rpt_product_performance.sql
│   │   │       └── rpt_customer_sales.sql
│   ├── tests/
│   │   ├── test_duplicate_transaction_ids.sql
│   │   ├── test_negative_sales.sql
│   │   ├── test_invalid_quantity.sql
│   │   ├── test_missing_product_category.sql
│   │   └── test_future_transactions.sql
│   ├── dbt_project.yml
│   └── README.md
│
├── dbt_profiles/
│   └── profiles.yml
│
├── venv/
├── .dockerignore
├── .env
├── .gitignore
├── Dockerfile
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

Astro CLI

### 2. Clone Repository & Create Virtual Environment
```bash
git clone <repo-url>
cd bridgestone_dw
```
- Create a virtual environment:
```bash
python -m venv venv
```
- Activate the virtual environment:

**Windows:**

```bash
venv\Scripts\activate
```
**macOS/Linux:**

```bash
source venv/bin/activate
```
- Install dependencies:

```bash
pip install -r requirements.txt
```
### 3. Snowflake Configuration
- Run the following SQL as ACCOUNTADMIN:
```sql
-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS BRIDGESTONE_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE;

-- Database and schemas
CREATE DATABASE IF NOT EXISTS BRIDGESTONE_DW;
USE DATABASE BRIDGESTONE_DW;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS MARTS;

-- dbt role
CREATE ROLE IF NOT EXISTS DBT_ROLE;
GRANT USAGE ON WAREHOUSE BRIDGESTONE_WH TO ROLE DBT_ROLE;
GRANT ALL ON DATABASE BRIDGESTONE_DW TO ROLE DBT_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE BRIDGESTONE_DW TO ROLE DBT_ROLE;

-- dbt user
CREATE USER IF NOT EXISTS DBT_USER
  PASSWORD = 'StrongPass123!'
  DEFAULT_ROLE = DBT_ROLE
  DEFAULT_WAREHOUSE = BRIDGESTONE_WH;

GRANT ROLE DBT_ROLE TO USER DBT_USER;
```
- Create a .env file in the project root:

.env
```text
SNOWFLAKE_ACCOUNT=<your_account>
SNOWFLAKE_USER=DBT_USER
SNOWFLAKE_PASSWORD=<your_password>
SNOWFLAKE_ROLE=DBT_ROLE
SNOWFLAKE_WAREHOUSE=BRIDGESTONE_WH
SNOWFLAKE_DATABASE=BRIDGESTONE_DW
SNOWFLAKE_SCHEMA=STAGING
AIRBYTE_API_TOKEN=<your_airbyte_token>
AIRBYTE_CONNECTION_ID=<your_connection_id>
FERNET_KEY=<generated_fernet_key>
```
### 4. Airbyte Configuration

Configure the following settings in Airbyte Cloud:

Source: Google Drive

Authenticate using a service account
Select folder: /raw_csv/
Glob pattern: *.csv

Destination: Snowflake

Database: BRIDGESTONE_DW
Schema: RAW
Table: STORE_FILES_RAW

Sync Configuration:

Sync Mode: Incremental | Append + Deduped
Frequency: Daily
Primary Keys: Product_id and Transaction_id

To modify connection settings after the initial setup, navigate to the relevant connection, open the Schema tab, select the desired stream, and update the configuration as needed.

### 5. dbt Setup
Install dbt:

```bash
pip install dbt-snowflake
```

- Navigate to dbt project:

```bash
cd dbt_project
Test connection:
```
```bash
dbt debug
Run models:
```
```bash
dbt run
Run tests:
```
```bash
dbt test
Generate documentation:
```
```bash
dbt docs generate
dbt docs serve
```
### 6. Airflow Orchestration with Astro
Initialize Astro (if not already done):

```bash
astro dev init
Update .astro/config.yaml:
```
```yaml
project:
  name: bridgestone-dw
  env:
    - AIRBYTE_API_TOKEN
    - AIRBYTE_CONNECTION_ID
    - SNOWFLAKE_USER
    - SNOWFLAKE_PASSWORD
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_ROLE
    - SNOWFLAKE_WAREHOUSE
    - SNOWFLAKE_DATABASE
    - FERNET_KEY
Start Airflow:
```
```bash
astro dev start
```
Access Airflow UI:

URL: http://localhost:8080

Username: admin

Password: admin

### 7. Run the Pipeline
Navigate to Airflow UI

Enable the bridgestone_pipeline DAG

Trigger the DAG manually or wait for scheduled run

Pipeline Flow:

```text
Airbyte Sync → Wait for Completion → dbt run (staging → intermediate → marts) → dbt test
```
## Data Models

### Staging Layer (STAGING schema - views)

| Model | Description |
| :--- | :--- |
| `stg_store_files` | Deduplicated, cleaned raw sales transactions with standardized data types |

### Intermediate Layer (INTERMEDIATE schema - views)

| Model | Description |
| :--- | :--- |
| `int_store_unioned` | Enriched staging model with calculated amount, date parts, and transaction date |

### Marts Layer (MARTS schema - tables)

**Dimensions:**

| Model | Description |
| :--- | :--- |
| `dim_stores` | Unique store locations extracted from sales data |
| `dim_products` | Product catalog with categories |
| `dim_customers` | Customer dimension (scaffold for future enrichment) |
| `dim_date` | Date spine (2020-2031) with calendar attributes |

**Fact:**

| Model | Description |
| :--- | :--- |
| `fct_store_sales` | Transaction-level fact table with foreign keys to all dimensions |

**Reporting:**

| Model | Description |
| :--- | :--- |
| `rpt_sales_summary` | Daily sales KPIs |
| `rpt_monthly_store_sales` | Monthly store performance |
| `rpt_product_performance` | Monthly product/category analysis |
| `rpt_customer_sales` | Customer lifetime value summary |

### Model Lineage Visualization

```text
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              MODEL LINEAGE DIAGRAM                                   │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  [RAW] ─────────────────────────────────────────────────────────────────────────┐   │
│  STORE_FILES_RAW                                                                 │   │
│         │                                                                        │   │
│         ▼                                                                        │   │
│  ┌──────────────────┐                                                           │   │
│  │   STAGING Layer  │                                                           │   │
│  │ stg_store_files  │                                                           │   │
│  └────────┬─────────┘                                                           │   │
│           │                                                                      │   │
│           ▼                                                                      │   │
│  ┌──────────────────┐                                                           │   │
│  │ INTERMEDIATE Layer│                                                          │   │
│  │ int_store_unioned │                                                          │   │
│  └────────┬─────────┘                                                           │   │
│           │                                                                      │   │
│     ┌─────┴─────┬─────────────┬─────────────┐                                  │   │
│     │           │             │             │                                  │   │
│     ▼           ▼             ▼             ▼                                  │   │
│ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐                                 │   │
│ │  dim_   │ │  dim_   │ │  dim_   │ │  dim_   │                                 │   │
│ │ stores  │ │products │ │customers│ │  date   │                                 │   │
│ └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘                                 │   │
│      │           │           │           │                                      │   │
│      └─────┬─────┴─────┬─────┴─────┬─────┘                                      │   │
│            │           │           │                                            │   │
│            ▼           ▼           ▼                                            │   │
│      ┌────────────────────────────────┐                                         │   │
│      │           FACT Table           │                                         │   │
│      │      fct_store_sales           │                                         │   │
│      └───────────────┬────────────────┘                                         │   │
│                      │                                                          │   │
│      ┌───────────────┼───────────────────────┬──────────────────┐              │   │
│      │               │                       │                  │              │   │
│      ▼               ▼                       ▼                  ▼              │   │
│ ┌────────────┐ ┌────────────┐ ┌──────────────────┐ ┌─────────────────┐         │   │
│ │    rpt_    │ │    rpt_    │ │      rpt_        │ │     rpt_        │         │   │
│ │   sales_   │ │  monthly_  │ │   product_       │ │   customer_     │         │   │
│ │  summary   │ │store_sales │ │  performance     │ │     sales       │         │   │
│ └────────────┘ └────────────┘ └──────────────────┘ └─────────────────┘         │   │
│                                                                                  │   │
└─────────────────────────────────────────────────────────────────────────────────┘   │
                                                                                      │
                                              ┌───────────────────────────────────────┘
                                              │
                                              ▼
                                  ┌─────────────────────┐
                                  │     POWER BI        │
                                  │    Dashboards       │
                                  └─────────────────────┘
```

## Data Dictionary

| Model | Layer | Description | Grain |
| :--- | :--- | :--- | :--- |
| `stg_store_files` | Staging | Deduplicated, cleaned raw sales transactions from POS system files | 1 row per transaction_id + product_id |
| `int_store_unioned` | Intermediate | Enriched staging model adding calculated amount, date parts, and transaction date | 1 row per transaction_id + product_id |
| `dim_stores` | Analytics | Unique store locations extracted from sales data | 1 row per store_id |
| `dim_products` | Analytics | Product dimension with name and category attributes | 1 row per product_id |
| `dim_customers` | Analytics | Customer dimension (scaffold for future enrichment) | 1 row per customer_id |
| `dim_date` | Analytics | Date spine (2020–2031) with calendar attributes & Power BI-friendly keys | 1 row per calendar day |
| `fct_store_sales` | Analytics | Fact table of store transactions linked to date dimension | 1 row per transaction line item |
| `rpt_sales_summary` | Reporting | Daily sales KPIs: revenue, volume, transactions, unique customers | 1 row per date |
| `rpt_monthly_store_sales` | Reporting | Monthly store-level performance metrics | 1 row per store + year + month |
| `rpt_product_performance` | Reporting | Monthly product/category revenue and units sold | 1 row per product + category + year + month |
| `rpt_customer_sales` | Reporting | Customer lifetime value and transaction summary | 1 row per customer_id |

## Column Details by Model

### `stg_store_files` (Staging)

| Column | Data Type | Description |
| :--- | :--- | :--- |
| `transaction_id` | VARCHAR | Unique transaction identifier |
| `store_id` | VARCHAR | Store location identifier |
| `product_id` | VARCHAR | Product identifier |
| `customer_id` | VARCHAR | Customer identifier |
| `transaction_timestamp` | TIMESTAMP | Date and time of transaction |
| `product_name` | VARCHAR | Name of the product |
| `category` | VARCHAR | Product category |
| `quantity` | INTEGER | Number of units purchased |
| `unit_price` | FLOAT | Price per unit |
| `total_amount` | FLOAT | Total transaction amount |
| `payment_method` | VARCHAR | Method of payment (standardized) |
| `_airbyte_extracted_at` | TIMESTAMP | Airbyte sync timestamp |

### `dim_date` (Analytics)

| Column | Data Type | Description |
| :--- | :--- | :--- |
| `date_key` | NUMBER | Surrogate key (YYYYMMDD format) |
| `date` | DATE | Natural date |
| `year` | INTEGER | Calendar year |
| `month` | INTEGER | Calendar month (1-12) |
| `day` | INTEGER | Day of month |
| `quarter` | INTEGER | Calendar quarter (1-4) |
| `month_name` | VARCHAR | Full month name (e.g., "January") |
| `day_name` | VARCHAR | Day name (e.g., "Mon") |
| `day_of_week` | INTEGER | ISO day of week (1=Monday, 7=Sunday) |
| `is_weekend` | BOOLEAN | TRUE if Saturday or Sunday |
| `quarter_name` | VARCHAR | Quarter label (Q1, Q2, Q3, Q4) |
| `year_month` | VARCHAR | Year-month formatted (YYYY-MM) |
| `year_month_key` | NUMBER | Numeric year-month (YYYYMM) |
| `month_number` | INTEGER | Month number for sorting (1-12) |
| `weekday_number` | INTEGER | Weekday number (1-7) |
| `is_today` | BOOLEAN | TRUE if date equals current date |
| `is_current_year` | BOOLEAN | TRUE if year equals current year |

## Macros

| Macro | Purpose | Key Parameters | Example Usage |
| :--- | :--- | :--- | :--- |
| `generate_schema_name` | Dynamically assigns schema names; falls back to target schema if none provided | `custom_schema_name`, `node` | `{{ generate_schema_name('marts', node) }}` |
| `generate_surrogate_key` | Creates an MD5 hash from a list of columns for consistent surrogate key generation | `columns` (list) | `{{ generate_surrogate_key(['store_id', 'date']) }}` |
| `audit_columns` | Adds `created_at` and `updated_at` timestamps for auditing | None | `{{ audit_columns() }}` |
| `standardize_payment_method` | Normalizes payment method strings to `'card'`, `'cash'`, `'transfer'`, or `'other'` | `column_name` | `{{ standardize_payment_method('payment_method') }}` |
| `generate_date_spine` | Generates a simple date spine using Snowflake's `generator` function | `start_date`, `num_days` | `{{ generate_date_spine('2020-01-01', 4018) }}` |
| `validate_total_amount` | Test macro that returns TRUE if `quantity * unit_price = total_amount` | `quantity`, `unit_price`, `total_amount` | `{{ validate_total_amount('qty', 'price', 'amount') }}` |
| `incremental_filter` | Applies incremental WHERE clause based on a timestamp column (only runs in incremental mode) | `timestamp_column` | `{{ incremental_filter('_airbyte_extracted_at') }}` |
| `null_if_blank` | Converts empty strings to SQL NULL after trimming whitespace | `column_name` | `{{ null_if_blank('customer_id') }}` |
| `calculate_avg_order_value` | Wrapper around `safe_divide` for average order value calculation | `sales`, `transactions` | `{{ calculate_avg_order_value('total_sales', 'transactions') }}` |
| `safe_divide` | Prevents division by zero; returns NULL if denominator is 0 | `numerator`, `denominator` | `{{ safe_divide('revenue', 'units') }}` |

## Macro Dependency Graph
```text
┌─────────────────────────────────────────────────────────────────┐
│                      MACRO DEPENDENCIES                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────┐                                        │
│  │  safe_divide        │                                        │
│  └──────────┬──────────┘                                        │
│             │                                                   │
│             ▼                                                   │
│  ┌─────────────────────┐                                        │
│  │calculate_avg_order_ │◄─────── Depends on safe_divide        │
│  │      value          │                                        │
│  └─────────────────────┘                                        │
│                                                                  │
│  ┌─────────────────────┐                                        │
│  │validate_total_amount│                                        │
│  └─────────────────────┘                                        │
│                                                                  │
│  ┌─────────────────────┐     ┌─────────────────────┐           │
│  │  incremental_filter │     │  generate_date_spine│           │
│  └─────────────────────┘     └─────────────────────┘           │
│                                                                  │
│  ┌─────────────────────┐     ┌─────────────────────┐           │
│  │   null_if_blank     │     │standardize_payment_ │           │
│  └─────────────────────┘     │      method         │           │
│                              └─────────────────────┘           │
│                                                                  │
│  ┌─────────────────────┐     ┌─────────────────────┐           │
│  │ generate_surrogate_ │     │    audit_columns    │           │
│  │       key           │     └─────────────────────┘           │
│  └─────────────────────┘                                        │
│                                                                  │
│  ┌─────────────────────┐                                        │
│  │ generate_schema_    │                                        │
│  │      name           │                                        │
│  └─────────────────────┘                                        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```
## Tests

| Test | Purpose | Severity | Applied To |
| :--- | :--- | :--- | :--- |
| `test_duplicate_transaction_ids` | Ensures no duplicate transaction_ids exist in the fact table | Error | `fct_store_sales` |
| `test_negative_sales` | Catches records with negative `total_amount` (invalid) | Error | `fct_store_sales` |
| `test_invalid_quantity` | Ensures `quantity` is greater than 0 | Error | `fct_store_sales` |
| `test_missing_product_category` | Flags products where `category` is NULL | Warn | `dim_products` |
| `test_future_transactions` | Detects transactions with a date later than current date | Error | `fct_store_sales` |

## Generic Tests (defined in schema.yml)

| Test Type | Description | Applied To |
| :--- | :--- | :--- |
| `unique` | Ensures all values in a column are distinct | `transaction_id`, `store_id`, `product_id`, `customer_id` |
| `not_null` | Ensures no NULL values exist in a column | All primary keys and required foreign keys |
| `accepted_values` | Validates column values against an allowed list | `payment_method`, `category` |
| `relationships` | Verifies foreign key integrity between fact and dimension tables | All FK relationships in `fct_store_sales` |

Test Execution Flow
```text
┌─────────────────────────────────────────────────────────────────┐
│                       TEST EXECUTION ORDER                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                     dbt test                             │    │
│  └─────────────────────────┬───────────────────────────────┘    │
│                            │                                     │
│                            ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           1. Generic Tests (schema.yml)                  │    │
│  │    - unique, not_null, accepted_values, relationships    │    │
│  └─────────────────────────┬───────────────────────────────┘    │
│                            │                                     │
│                            ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           2. Singular Tests (tests/ folder)              │    │
│  │    - Business logic and custom validation                │    │
│  └─────────────────────────┬───────────────────────────────┘    │
│                            │                                     │
│                            ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    3. Results                            │    │
│  │    - PASS: All tests successful                          │    │
│  │    - FAIL: Specific tests highlighted with row details   │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```
## Running Commands
### dbt Commands
bash
### Navigate to dbt project
cd dbt_project

### Run all models
```bash
dbt run
```
### Run specific models
```bash
dbt run --select staging
dbt run --select marts
dbt run --select reporting
```
### Running Tests
```bash
# Run all tests
dbt test

# Run specific test by name
dbt test --select test_duplicate_transaction_ids

# Run tests for specific model
dbt test --select fct_store_sales

# Run tests with store failures
dbt test --store-failures

# Run tests and output to file
dbt test --log-format json > test_results.json
```
## Generate documentation
```bash
dbt docs generate
dbt docs serve
```
## Debug connection
```bash
dbt debug
```
## Clean dbt artifacts
```bash
dbt clean
```
## Airflow Commands
bash
### Start Airflow
```bash
astro dev start
```
### Stop Airflow
```bash
astro dev stop
```

### Restart Airflow
```bash
astro dev restart
```
### View logs
```bash
astro dev logs
```

### Kill all running containers
```bash
astro dev kill
```

## Power BI Integration
### Recommended Connection Method

1. **DirectQuery** for detailed transaction tables (`fct_store_sales`)
2. **Import** for reporting tables (`rpt_*` models) for maximum performance

### Dashboard Specifications

| Dashboard | Source Tables | Key Metrics | Typical Use Case |
| :--- | :--- | :--- | :--- |
| Executive Summary | `rpt_sales_summary`, `dim_date` | Total sales, transaction count, unique customers, average order value | C-suite reporting, daily KPI monitoring |
| Store Performance | `rpt_monthly_store_sales`, `dim_stores` | Monthly sales by store, items sold, transaction volume per location | Regional managers, store operations |
| Product Analytics | `rpt_product_performance`, `dim_products` | Units sold, revenue by category, product rankings | Product management, inventory planning |
| Customer Insights | `rpt_customer_sales` | Lifetime value, transaction frequency, average transaction value | Marketing, loyalty programs, CRM |

### Dashboard Relationships

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                           POWER BI DATA MODEL                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐      ┌─────────────────────┐      ┌──────────────┐       │
│  │  dim_date    │      │  rpt_sales_summary  │      │  dim_stores  │       │
│  │              │◄─────│                     │      │              │       │
│  │  - date_key  │      │  - date (FK)        │      │  - store_id  │       │
│  │  - year      │      │  - total_sales      │      └──────┬───────┘       │
│  │  - month     │      │  - total_quantity   │             │               │
│  │  - quarter   │      │  - total_transactions│             │               │
│  └──────────────┘      │  - unique_customers  │             │               │
│         │              └─────────────────────┘             │               │
│         │                                                  │               │
│         │              ┌─────────────────────┐             │               │
│         │              │ rpt_monthly_store_  │             │               │
│         └─────────────►│      sales          │◄────────────┘               │
│                        │                     │                             │
│                        │  - year (FK)        │                             │
│                        │  - month (FK)       │                             │
│                        │  - store_id (FK)    │                             │
│                        │  - monthly_sales    │                             │
│                        │  - items_sold       │                             │
│                        │  - transactions     │                             │
│                        └─────────────────────┘                             │
│                                                                              │
│  ┌──────────────┐      ┌─────────────────────┐                             │
│  │  dim_products│      │ rpt_product_perf    │                             │
│  │              │      │                     │                             │
│  │  - product_id│◄─────│  - product_id (FK)  │                             │
│  │  - product   │      │  - year (FK)        │                             │
│  │    name      │      │  - month (FK)       │                             │
│  │  - category  │      │  - category         │                             │
│  └──────────────┘      │  - units_sold       │                             │
│                        │  - revenue          │                             │
│                        └─────────────────────┘                             │
│                                                                              │
│                        ┌─────────────────────┐                             │
│                        │  rpt_customer_sales │                             │
│                        │                     │                             │
│                        │  - customer_id      │                             │
│                        │  - transaction_count│                             │
│                        │  - lifetime_value   │                             │
│                        │  - avg_transaction_ │                             │
│                        │    value            │                             │
│                        └─────────────────────┘                             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```
### Power BI-Friendly Features Built-in
date_key as YYYYMMDD numeric for fast relationships

year_month_key for time intelligence

Pre-aggregated reporting tables for instant visuals

Boolean flags (is_weekend, is_today, is_current_year)

## Troubleshooting Guide: dbt, Airbyte, Airflow, Snowflake
---
| Issue | Solution |
|-------|----------|
| **dbt cannot connect to Snowflake** | Check `.env` variables and `profiles.yml` |
| **Airbyte sync fails** | Verify Google Drive credentials and file paths |
| **Airflow DAG fails** | Check logs: `astro dev logs --follow` |
| **Duplicate records in staging** | Verify `row_number()` partition columns |
| **Permission denied in Snowflake** | Run `ACCOUNTADMIN GRANT` statements |

---

## Detailed Resolution Steps

### 1. dbt cannot connect to Snowflake
- **Check:** Ensure environment variables (e.g., `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_DATABASE`) are correctly set.
- **Check:** Validate `profiles.yml` syntax and target schema.
- **Test connection:** Run `dbt debug` to get detailed error logs.

### 2. Airbyte sync fails
- **Check:** Google Drive credentials (OAuth or service account) are still valid.
- **Check:** Source file paths have not been moved or renamed.
- **Check:** Airbyte connector version is up-to-date.

### 3. Airflow DAG fails
- **View logs:** Use `astro dev logs --follow` for real-time log streaming.
- **Check:** Task dependencies, variable references, and connection IDs in Airflow UI.
- **Restart:** `astro dev restart` if the scheduler is unresponsive.

### 4. Duplicate records in staging
- **Verify:** `row_number()` window function is partitioned by **all unique key columns** (e.g., `PARTITION BY id, date`).
- **Verify:** `QUALIFY row_number() = 1` or similar deduplication logic is applied.
- **Check for:** Multiple runs of the same pipeline without proper incremental logic.

### 5. Permission denied in Snowflake
- **Run as ACCOUNTADMIN:** Use a role with `ACCOUNTADMIN` privileges.
- **Grant necessary permissions:**
  ```sql
  GRANT USAGE ON DATABASE your_db TO ROLE your_role;
  GRANT USAGE ON SCHEMA your_schema TO ROLE your_role;
  GRANT SELECT ON ALL TABLES IN SCHEMA your_schema TO ROLE your_role;
  ```

## Useful Debug Queries

```sql
-- Check row counts by schema
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    ROW_COUNT
FROM BRIDGESTONE_DW.INFORMATION_SCHEMA.TABLES
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- Validate date spine range
SELECT MIN(date), MAX(date) FROM BRIDGESTONE_DW.MARTS.DIM_DATE;

-- Check for orphaned fact records

SELECT COUNT(*) 
FROM BRIDGESTONE_DW.MARTS.FCT_STORE_SALES f
LEFT JOIN BRIDGESTONE_DW.MARTS.DIM_DATE d ON f.date_key = d.date_key
WHERE d.date_key IS NULL;
```

## Need More Help?

- **dbt:** [dbt Community Slack](https://community.getdbt.com/)
- **Airbyte:** [Airbyte Docs](https://docs.airbyte.com/)
- **Airflow:** [Astro CLI Docs](https://docs.astronomer.io/astro/cli-overview)
- **Snowflake:** [Snowflake Support](https://support.snowflake.com/)

## Future Enhancements
Add CI/CD pipeline with GitHub Actions

Integrate data quality monitoring (Great Expectations / Elementary)

Add Slack or email alerting for failed tests

Optimize Snowflake compute with auto-suspend policies

Implement Slowly Changing Dimensions (SCD Type 2) for customer dimension

Add additional data sources (online orders, inventory)

Implement row-level access control for sensitive data

Add data lineage visualization in Power BI

## Contributing
Create a feature branch

Make changes to dbt models

Run dbt run and dbt test locally

Submit a pull request with description of changes

Ensure all tests pass before merging

License
This project is proprietary and confidential.

Author
Binah Utuedor
Lead Data Architect & Senior Data Engineer

