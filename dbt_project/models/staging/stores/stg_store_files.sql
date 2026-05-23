{{ config(materialized='view') }}

with ranked as (

    select *,
           row_number() over (
               partition by transaction_id, product_id
               order by _airbyte_extracted_at desc
           ) as rn
    from {{ source('raw', 'STORE_FILES_RAW') }}

),

cleaned as (

    select
        transaction_id,
        store_id,
        product_id,
        customer_id,

        try_to_timestamp(transaction_timestamp)
            as transaction_timestamp,

        trim(product_name)
            as product_name,

        trim(category)
            as category,

        cast(quantity as integer)
            as quantity,

        cast(unit_price as float)
            as unit_price,

        cast(total_amount as float)
            as total_amount,

        lower(payment_method)
            as payment_method,

        _airbyte_extracted_at

    from ranked
    where rn = 1

)

select *
from cleaned