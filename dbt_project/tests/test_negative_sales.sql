-- tests/test_negative_sales.sql

select *
from {{ ref('fct_store_sales') }}
where total_amount < 0