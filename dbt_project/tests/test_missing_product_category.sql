-- tests/test_missing_product_category.sql

select *
from {{ ref('dim_products') }}
where category is null