{{ config(materialized='table') }}

select
    -- time dimensions for monthly reporting
    d.year,
    d.month,

    -- product attributes for sales analysis
    p.category,
    p.product_name,

    -- product sales performance metrics
    sum(f.quantity) as units_sold,
    sum(f.total_amount) as revenue

from {{ ref('fct_store_sales') }} f

-- join product dimension for product details and categorization
join {{ ref('dim_products') }} p
    on f.product_id = p.product_id

-- join date dimension for calendar attributes
join {{ ref('dim_date') }} d
    on f.date_key = d.date_key

-- aggregate metrics at the month-product level
group by
    d.year,
    d.month,
    p.category,
    p.product_name