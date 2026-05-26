{{ config(materialized='table') }}

select
    -- calendar date for daily reporting
    d.date,

    -- daily sales and customer activity metrics
    sum(f.total_amount) as total_sales,
    sum(f.quantity) as total_quantity,
    count(distinct f.transaction_id) as total_transactions,
    count(distinct f.customer_id) as unique_customers

from {{ ref('fct_store_sales') }} f

-- join date dimension for calendar attributes
join {{ ref('dim_date') }} d
    on f.date_key = d.date_key

-- aggregate metrics at the daily level
group by d.date