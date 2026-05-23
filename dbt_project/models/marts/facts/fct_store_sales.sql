{{ config(materialized='table') }}

select
    transaction_id,
    store_id,
    customer_id,
    product_id,

    transaction_timestamp,
    transaction_date,

    quantity,
    unit_price,
    total_amount,

    -- date key for joins
    to_number(to_char(transaction_timestamp, 'YYYYMMDD')) as date_key

from {{ ref('int_store_unioned') }}