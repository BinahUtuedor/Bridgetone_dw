{{ config(materialized='view') }}

with base as (

    select
        transaction_id,
        store_id,
        product_id,
        customer_id,
        transaction_timestamp,
        product_name,
        category,
        quantity,
        unit_price,
        total_amount,
        payment_method,
        _airbyte_extracted_at

    from {{ ref('stg_store_files') }}

)

select
    transaction_id,
    store_id,
    product_id,
    customer_id,
    transaction_timestamp,
    product_name,
    category,
    quantity,
    unit_price,
    total_amount,
    payment_method,
    _airbyte_extracted_at,

    quantity * unit_price as calculated_amount,

    date(transaction_timestamp)
        as transaction_date,

    extract(year from transaction_timestamp)
        as year,

    extract(month from transaction_timestamp)
        as month

from base