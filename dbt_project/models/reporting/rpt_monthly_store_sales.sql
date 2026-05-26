{{ config(materialized='table') }}

select
    -- time dimensions for monthly reporting
    d.year,
    d.month,

    -- store identifier
    f.store_id,

    -- monthly sales performance metrics
    sum(f.total_amount) as monthly_sales,
    sum(f.quantity) as items_sold,
    count(distinct f.transaction_id) as transactions

from {{ ref('fct_store_sales') }} f

-- join to date dimension for calendar attributes
join {{ ref('dim_date') }} d
    on f.date_key = d.date_key

-- aggregate metrics at the store-month level
group by
    d.year,
    d.month,
    f.store_id