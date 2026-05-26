{{ config(materialized='table') }}

select
    -- transaction identifiers and dimensions
    transaction_id,
    store_id,
    customer_id,
    product_id,

    -- transaction timestamps
    transaction_timestamp,
    transaction_date,

    -- transaction metrics
    quantity,
    unit_price,
    total_amount,

    -- surrogate date key (YYYYMMDD format) for joining to date dimension
    to_number(to_char(transaction_timestamp, 'YYYYMMDD')) as date_key

from {{ ref('int_store_unioned') }}