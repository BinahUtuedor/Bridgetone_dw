{{ config(materialized='table') }}

select
    date,
    sum(total_amount) as total_sales,
    sum(quantity) as total_quantity,
    count(distinct transaction_id) as total_transactions,
    count(distinct customer_id) as unique_customers
from {{ ref('fct_store_sales') }}
group by date