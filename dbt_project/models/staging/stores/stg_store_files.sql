{{ config(materialized='view') }}

-- Deduplicate records coming from the raw source by keeping
-- only the latest version of each transaction/product pair
with ranked as (

    select *,
           
           -- Assign a row number ordered by extraction time
           -- so the most recently extracted record gets rn = 1
           row_number() over (
               partition by transaction_id, product_id
               order by _airbyte_extracted_at desc
           ) as rn

    from {{ source('raw', 'STORE_FILES_RAW') }}

),

-- Clean and standardize fields for downstream analytics
cleaned as (

    select
        
        -- Core identifiers
        transaction_id,
        store_id,
        product_id,
        customer_id,

        -- Convert transaction timestamp into proper timestamp type
        try_to_timestamp(transaction_timestamp)
            as transaction_timestamp,

        -- Remove leading/trailing whitespace from text fields
        trim(product_name)
            as product_name,

        trim(category)
            as category,

        -- Convert numeric values into expected data types
        cast(quantity as integer)
            as quantity,

        cast(unit_price as float)
            as unit_price,

        cast(total_amount as float)
            as total_amount,

        -- Normalize payment method values to lowercase
        lower(payment_method)
            as payment_method,

        -- Keep metadata column for lineage/debugging
        _airbyte_extracted_at

    from ranked

    -- Keep only the most recent record per transaction/product
    where rn = 1

)

-- Return final cleaned dataset
select *
from cleaned