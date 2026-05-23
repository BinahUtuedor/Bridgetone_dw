{{ config(materialized='table') }}

select
    d.date,

    sum(f.total_amount) as total_sales,
    sum(f.quantity) as total_quantity,
    count(distinct f.transaction_id) as total_transactions,
    count(distinct f.customer_id) as unique_customers

from {{ ref('fct_store_sales') }} f
join {{ ref('dim_date') }} d
    on f.date_key = d.date_key

group by d.date