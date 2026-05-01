{{ config(materialized='table') }}

select distinct
    store_id
from {{ ref('int_store_unioned') }}