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
Below is the ASCII architecture diagram.

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
│
├── .astro/
│       └──config.yaml
│       └──config.yaml.lock
│       └──dag_integrity_exceptions.txt
│       └──test_dag_integrity_default.py
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   └── stores/
│   │   │       ├── stg_store_files.sql
│   │   │       └── schema.yml
│   │   ├── intermediate/
│   │   │   └── int_store_unioned.sql
│   │   │
│   │   ├── marts/
│   │   │    ├── dimensions/
│   │   │    │    ├── dim_stores.sql
│   │   │    │    ├── dim_products.sql
│   │   │    │    ├── dim_customers.sql
│   │   │    │    └── dim_date.sql
│   │   │    └── facts/
│   │   │         └── fct_store_sales.sql
│   │   └── reporting/
│   │        └── rpt_sales_summary
│   │        └── rpt_monthly_store_sales
│   │        └── rpt_product_performance
│   │        └── rpt_customer_sales
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
│   ├── tests/
│   │     └── test_duplicate_transaction_ids.sql
│   │     └── test_negative_sales.sql
│   │     └── test_invalid_quantity.sql
│   │     └── test_missing_product_category.sql
│   │     └── test_future_transactions.sql
│   ├── dbt_project.yml
│   └── README.md
├── dags/
│    └──.airflowignore
│    └──exampledag.py
│    └── bridgestone_pipeline.py      # Airflow DAG
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

Configure the airflow DAG bridgestone_pipeline.py

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

Add a Reporting/Aggregate Layer?

Your current fct_store_sales table is:

transaction-grain
detailed
normalized for analytics flexibility

That is excellent for:

drilldowns
detailed analysis
data science
ad hoc queries

But dashboards often repeatedly calculate:

monthly sales
category summaries
top products
store KPIs

on millions of rows.

This becomes:

slower
more expensive
harder to maintain
Purpose of the Reporting Layer

The reporting layer contains:

pre-aggregated tables
KPI-ready datasets
business-facing models
dashboard-specific summaries

Think of it as:

“Power BI ready-to-consume datasets.”

Recommended Updated Structure
bridgestone_dw/
├── airflow/
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   ├── marts/
│   │   │   ├── dimensions/
│   │   │   └── facts/
│   │   └── reporting/
│   │       ├── rpt_sales_summary.sql
│   │       ├── rpt_monthly_store_sales.sql
│   │       ├── rpt_product_performance.sql
│   │       └── rpt_customer_sales.sql
Recommended Reporting Models
1. rpt_sales_summary.sql

High-level KPI table.

Purpose:

executive dashboard
KPI cards
daily monitoring

Example:

{{ config(materialized='table') }}

select
    date,
    sum(total_amount) as total_sales,
    sum(quantity) as total_quantity,
    count(distinct transaction_id) as total_transactions,
    count(distinct customer_id) as unique_customers
from {{ ref('fct_store_sales') }}
group by date
Why This Exists

Without this:

Power BI recalculates millions of rows repeatedly

With aggregates:

dashboards become extremely fast
2. rpt_monthly_store_sales.sql

Purpose:

regional/store performance dashboards

Example:

{{ config(materialized='table') }}

select
    d.year,
    d.month,
    f.store_id,

    sum(f.total_amount) as monthly_sales,
    sum(f.quantity) as items_sold,
    count(distinct f.transaction_id) as transactions

from {{ ref('fct_store_sales') }} f
join {{ ref('dim_date') }} d
    on f.date_key = d.date_key

group by
    d.year,
    d.month,
    f.store_id
Benefits

This allows Power BI to directly query:

monthly totals
trends
store rankings

without recalculating raw transactions.

3. rpt_product_performance.sql

Purpose:

product/category analysis
inventory insights
best sellers

Example:

{{ config(materialized='table') }}

select
    d.year,
    d.month,

    p.category,
    p.product_name,

    sum(f.quantity) as units_sold,
    sum(f.total_amount) as revenue

from {{ ref('fct_store_sales') }} f
join {{ ref('dim_products') }} p
    on f.product_id = p.product_id
join {{ ref('dim_date') }} d
    on f.date_key = d.date_key

group by
    d.year,
    d.month,
    p.category,
    p.product_name
4. rpt_customer_sales.sql

Purpose:

customer spending analysis
segmentation
loyalty analytics

Example:

{{ config(materialized='table') }}

select
    customer_id,

    count(distinct transaction_id) as transaction_count,
    sum(total_amount) as lifetime_value,
    avg(total_amount) as avg_transaction_value

from {{ ref('fct_store_sales') }}

group by customer_id
Why This Layer Matters in Real Companies

Large organizations often separate:

Layer	Audience
Facts/Dimensions	Data engineers & analysts
Reporting Models	BI developers
Dashboards	Business users

This separation:

improves maintainability
standardizes KPIs
avoids duplicated calculations
improves performance
Materialization Strategy
Layer	Recommended Materialization
Staging	View
Intermediate	View
Dimensions	Table
Facts	Incremental/Table
Reporting	Table or Incremental

Why reporting models should usually be tables:

precomputed
optimized for BI queries
avoids recalculating aggregations

### Testing
dbt general tests include:

Schema tests: unique, not_null

Custom tests: business logic validations

Source freshness checks


Add singular tests
Step 1 — Create the Tests Folder

Inside your dbt project:

dbt_project/
├── tests/

Step 2 — Create a Singular Test File

Example:

tests/test_total_amount_matches.sql
Step 3 — Write the SQL Test

Example:

-- tests/test_total_amount_matches.sql

select *
from {{ ref('int_store_unioned') }}
where total_amount != calculated_amount

Purpose:

identifies mismatched transaction totals

If rows appear:

test fails
dbt reports problematic rows
Step 4 — Run the Test

Execute:

dbt test

Or run a specific test:

dbt test --select test_total_amount_matches
1. Recommended Singular Tests for Your Project
A. Duplicate Transaction IDs
-- tests/test_duplicate_transaction_ids.sql

select
    transaction_id,
    count(*) as cnt
from {{ ref('fct_store_sales') }}
group by transaction_id
having count(*) > 1

Purpose:

ensures transaction uniqueness
B. Negative Sales Amounts
-- tests/test_negative_sales.sql

select *
from {{ ref('fct_store_sales') }}
where total_amount < 0

Purpose:

catches invalid sales data
C. Invalid Quantities
-- tests/test_invalid_quantity.sql

select *
from {{ ref('fct_store_sales') }}
where quantity <= 0

Purpose:

prevents impossible sales quantities
D. Missing Product Categories
-- tests/test_missing_product_category.sql

select *
from {{ ref('dim_products') }}
where category is null

Purpose:

ensures product classification quality
E. Future Transaction Dates
-- tests/test_future_transactions.sql

select *
from {{ ref('fct_store_sales') }}
where date > current_date

Purpose:

catches timestamp/data entry issues
6. Difference Between Singular and Generic Tests
Generic Tests	Singular Tests
Prebuilt	Custom SQL
Defined in YAML	Defined in SQL
Reusable	Highly specific
Examples: unique, not_null	Business rules

Run tests:

dbt test

Add macros

1. Surrogate Key Macro

One of the most important macros.

File
macros/surrogate_keys.sql
Macro
{% macro generate_surrogate_key(columns) %}

md5(
    {% for column in columns %}
        coalesce(cast({{ column }} as varchar), '')
        {% if not loop.last %} || '|' || {% endif %}
    {% endfor %}
)

{% endmacro %}
Why This Is Important

Used for:

dimension surrogate keys
deduplication
SCD handling
composite business keys
Example Usage
{{ generate_surrogate_key([
    'store_id',
    'product_id',
    'transaction_timestamp'
]) }} as sales_sk
2. Audit Columns Macro

Very common enterprise pattern.

File
macros/auditing.sql
Macro
{% macro audit_columns() %}

current_timestamp() as created_at,
current_timestamp() as updated_at

{% endmacro %}
Usage
select
    transaction_id,
    total_amount,

    {{ audit_columns() }}

from sales
Why Useful

Adds:

lineage
auditing
warehouse governance
debugging capability
3. Safe Division Macro

Prevents divide-by-zero errors.

File
macros/utilities.sql
Macro
{% macro safe_divide(numerator, denominator) %}

case
    when {{ denominator }} = 0 then null
    else {{ numerator }} / {{ denominator }}
end

{% endmacro %}
Usage
{{ safe_divide('total_sales', 'transaction_count') }}
    as avg_order_value
Why Important

Very common in:

KPI calculations
reporting models
finance metrics
4. Standardized Payment Method Macro

Centralize business logic.

File
macros/formatting.sql
Macro
{% macro standardize_payment_method(column_name) %}

case
    when lower(trim({{ column_name }})) in ('card', 'credit card')
        then 'card'

    when lower(trim({{ column_name }})) in ('cash')
        then 'cash'

    when lower(trim({{ column_name }})) in ('transfer', 'bank transfer')
        then 'transfer'

    else 'other'
end

{% endmacro %}
Usage
{{ standardize_payment_method('payment_method') }}
    as payment_method
Why This Matters

Instead of duplicating:

case when ...

everywhere,
you centralize logic once.

Huge maintainability improvement.

5. Date Spine Macro

Reusable calendar generator.

File
macros/dates.sql
Macro
{% macro generate_date_spine(start_date, num_days) %}

select
    dateadd(day, seq4(), '{{ start_date }}') as date_day
from table(generator(rowcount => {{ num_days }}))

{% endmacro %}
Usage
with date_spine as (

    {{ generate_date_spine('2020-01-01', 4018) }}

)
Why Useful

Avoids hardcoding:

generator logic
repetitive date spine SQL
6. Amount Validation Macro

Useful for data quality.

File
macros/testing.sql
Macro
{% macro validate_total_amount(quantity, unit_price, total_amount) %}

(
    {{ quantity }} * {{ unit_price }}
) = {{ total_amount }}

{% endmacro %}
Usage in Tests
select *
from {{ ref('fct_store_sales') }}
where not (
    {{ validate_total_amount(
        'quantity',
        'unit_price',
        'total_amount'
    ) }}
)
7. Incremental Filter Macro

Very useful later.

File
macros/incremental.sql
Macro
{% macro incremental_filter(timestamp_column) %}

{% if is_incremental() %}

where {{ timestamp_column }} >
(
    select max({{ timestamp_column }})
    from {{ this }}
)

{% endif %}

{% endmacro %}
Usage
select *
from source_table

{{ incremental_filter('_airbyte_extracted_at') }}
Why Important

Supports:

faster dbt runs
production scalability
lower Snowflake costs

8. Null Handling Macro
File Name
macros/null_handling.sql
Macro
{% macro null_if_blank(column_name) %}

case
    when trim({{ column_name }}) = '' then null
    else trim({{ column_name }})
end

{% endmacro %}
Why This File Exists

Purpose:

standardize blank values
clean dirty ingestion data
improve data quality

Typical problems solved:

empty strings
whitespace-only values
inconsistent CSV ingestion
Usage Example

Inside stg_store_files.sql:

select
    {{ null_if_blank('customer_id') }} as customer_id,
    {{ null_if_blank('product_name') }} as product_name
from source
9. Generic KPI Macro
File Name
macros/kpi_calculations.sql
Macro
{% macro calculate_avg_order_value(sales, transactions) %}

{{ safe_divide(sales, transactions) }}

{% endmacro %}
Dependency

This macro depends on:

macros/utilities.sql

which contains:

{% macro safe_divide(numerator, denominator) %}

case
    when {{ denominator }} = 0 then null
    else {{ numerator }} / {{ denominator }}
end

{% endmacro %}

Why Separate KPI Macros into Their Own File

This improves:

readability
organization
business metric governance
semantic consistency

In enterprise projects:

KPI logic is centralized
reused across reporting models
reused across dashboards
Usage Example

Inside:

models/reporting/rpt_sales_summary.sql
select
    sum(total_amount) as total_sales,
    count(distinct transaction_id) as transaction_count,

    {{ calculate_avg_order_value(
        'sum(total_amount)',
        'count(distinct transaction_id)'
    ) }} as avg_order_value

from {{ ref('fct_store_sales') }}

Install astro to your local machine

Orchestration with Airflow
initialize Astro in your project
astro dev init
Start Airflow locally

After init, run:

astro dev start

## 📈 Future Enhancements
Add CI/CD (GitHub Actions)

Integrate data quality monitoring (Great Expectations / Elementary)

Add Slack or email alerting

Optimise Snowflake compute usage

## 👤 Author
Binah Utuedor  
Lead Data Architect & Senior Data Engineer