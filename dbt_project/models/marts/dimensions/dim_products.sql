-- Creates a product master table of unique products with their associated attributes
-- sourced from the unioned intermediate store model.
{{ config(materialized='table') }}

select distinct
    -- Unique product identifier
    product_id,

    -- Product display/name field
    product_name,

    -- Product category classification
    category
from {{ ref('int_store_unioned') }}