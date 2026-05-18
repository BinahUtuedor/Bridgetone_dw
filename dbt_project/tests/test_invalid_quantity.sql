-- tests/test_invalid_quantity.sql

select *
from {{ ref('fct_store_sales') }}
where quantity <= 0