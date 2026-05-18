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