{{ config(materialized='table') }}

select
    customer_id,

    count(distinct transaction_id) as transaction_count,
    sum(total_amount) as lifetime_value,
    avg(total_amount) as avg_transaction_value

from {{ ref('fct_store_sales') }}

group by customer_id