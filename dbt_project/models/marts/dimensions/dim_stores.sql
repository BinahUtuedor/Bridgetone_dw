-- Creates a table containing a deduplicated list of store identifiers
-- sourced from the unioned intermediate store model.
{{ config(materialized='table') }}

select distinct
    -- Return unique store IDs only
    store_id
from {{ ref('int_store_unioned') }}