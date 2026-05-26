{{ config(materialized='table') }}

select
    -- customer identifier
    customer_id,

    -- customer purchase behavior metrics
    count(distinct transaction_id) as transaction_count,
    sum(total_amount) as lifetime_value,
    avg(total_amount) as avg_transaction_value

from {{ ref('fct_store_sales') }}

-- aggregate metrics at the customer level
group by customer_id