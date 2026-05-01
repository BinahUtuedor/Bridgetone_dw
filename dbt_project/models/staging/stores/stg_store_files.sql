{{ config(materialized='view') }}

with source as (

    select * 
    from {{ source('raw', 'STORE_FILES_RAW') }}

),

cleaned as (

    select
        -- Primary identifiers
        transaction_id,
        store_id,
        product_id,
        customer_id,

        -- Timestamps
        try_to_timestamp(transaction_timestamp) as transaction_timestamp,

        -- Product info
        trim(product_name) as product_name,
        trim(category) as category,

        -- Measures
        cast(quantity as integer) as quantity,
        cast(unit_price as float) as unit_price,
        cast(total_amount as float) as total_amount,

        -- Payment
        lower(payment_method) as payment_method,

        -- Metadata
        _airbyte_extracted_at

    from source

)

select * from cleaned