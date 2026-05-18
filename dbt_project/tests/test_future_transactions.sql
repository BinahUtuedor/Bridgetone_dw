-- tests/test_future_transactions.sql

select *
from {{ ref('fct_store_sales') }}
where date > current_date