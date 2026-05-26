-- tests/test_missing_product_category.sql

-- Data quality test to ensure all products have a valid category
-- This test identifies records in dim_products where category is missing (NULL)
-- If any rows are returned, the test FAILS
-- If no rows are returned, the test PASSES

select *
from {{ ref('dim_products') }}

-- Filters for products where category has not been populated
where category is null