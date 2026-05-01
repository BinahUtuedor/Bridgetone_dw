{{ config(materialized='table') }}

select distinct
    customer_id
from {{ ref('int_store_unioned') }}