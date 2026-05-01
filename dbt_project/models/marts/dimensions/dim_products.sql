{{ config(materialized='table') }}

select distinct
    product_id,
    product_name,
    category
from {{ ref('int_store_unioned') }}