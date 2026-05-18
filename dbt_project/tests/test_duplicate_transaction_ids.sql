-- tests/test_duplicate_transaction_ids.sql

select
    transaction_id,
    count(*) as cnt
from {{ ref('fct_store_sales') }}
group by transaction_id
having count(*) > 1