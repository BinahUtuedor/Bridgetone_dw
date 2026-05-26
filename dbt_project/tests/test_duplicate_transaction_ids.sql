-- tests/test_duplicate_transaction_ids.sql

-- Data quality test to ensure transaction_id is unique in fct_store_sales
-- This query returns ONLY duplicate transaction_ids
-- If any rows are returned, the dbt test FAILS
-- If no rows are returned, the dbt test PASSES

select
    transaction_id,
    count(*) as cnt  -- number of occurrences for each transaction_id
from {{ ref('fct_store_sales') }}

group by transaction_id  -- groups rows so we can evaluate duplicates per transaction

having count(*) > 1      -- filters to only keep transaction_ids that appear more than once