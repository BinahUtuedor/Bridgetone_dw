{{ config(materialized='table') }}

select
    -- Primary key
    transaction_id,

    -- 🔑 New: date surrogate key (for dim_date)
    to_number(to_char(cast(transaction_timestamp as date), 'YYYYMMDD')) as date_key,

    -- Keep existing date (DO NOT remove)
    cast(transaction_timestamp as date) as date,

    -- Existing foreign keys (UNCHANGED)
    store_id,
    product_id,
    customer_id,

    -- Measures
    quantity,
    unit_price,
    total_amount,

    -- Degenerate dimension
    payment_method

from {{ ref('int_store_unioned') }}