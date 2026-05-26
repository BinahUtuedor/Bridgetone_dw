-- Creates a deduplicated list of customers across all store sources
-- Materialize this model as a table in the warehouse
{{ config(materialized='table') }}

select distinct
    customer_id
from {{ ref('int_store_unioned') }} -- Reference the intermediate unioned store model

