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