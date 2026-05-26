{{ config(materialized='view') }}

-- Pull standardized transaction data from the staging model
with base as (

    select
        
        -- Transaction and entity identifiers
        transaction_id,
        store_id,
        product_id,
        customer_id,

        -- Transaction details
        transaction_timestamp,
        product_name,
        category,

        -- Sales metrics
        quantity,
        unit_price,
        total_amount,

        -- Payment information
        payment_method,

        -- Metadata for lineage/debugging
        _airbyte_extracted_at

    from {{ ref('stg_store_files') }}

)

select
    
    -- Core identifiers
    transaction_id,
    store_id,
    product_id,
    customer_id,

    -- Transaction attributes
    transaction_timestamp,
    product_name,
    category,

    -- Sales values
    quantity,
    unit_price,
    total_amount,

    -- Payment details
    payment_method,

    -- Source extraction timestamp
    _airbyte_extracted_at,

    -- Recalculate amount for validation or comparison purposes
    quantity * unit_price as calculated_amount,

    -- Extract transaction date (without time component)
    date(transaction_timestamp)
        as transaction_date,

    -- Derive year for reporting and aggregations
    extract(year from transaction_timestamp)
        as year,

    -- Derive month for reporting and aggregations
    extract(month from transaction_timestamp)
        as month

from base