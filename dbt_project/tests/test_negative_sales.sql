-- tests/test_negative_sales.sql

-- Data quality test to identify invalid negative sales amounts in fct_store_sales
-- Sales values should not be negative in a standard retail context
-- If any rows are returned, the test FAILS
-- If no rows are returned, the test PASSES

select *
from {{ ref('fct_store_sales') }}

-- Filters for records where total_amount is less than zero
-- These represent potentially incorrect, refunded, or corrupted transactions
where total_amount < 0