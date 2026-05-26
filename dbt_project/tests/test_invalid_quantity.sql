-- tests/test_invalid_quantity.sql

-- Data quality test to identify invalid quantity values in fct_store_sales
-- This test checks for rows where quantity is zero or negative
-- If any rows are returned, the test FAILS
-- If no rows are returned, the test PASSES

select *
from {{ ref('fct_store_sales') }}

-- Filters out records with invalid or nonsensical quantities
-- Quantity should typically be > 0 for valid sales transactions
where quantity <= 0