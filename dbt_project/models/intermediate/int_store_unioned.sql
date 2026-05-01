{{ config(materialized='view') }}

with base as (

    select * from {{ ref('stg_store_files') }}

),

enhanced as (

    select
        *,
        
        -- Derived metrics
        quantity * unit_price as calculated_amount,

        -- Date breakdown (VERY IMPORTANT for Power BI)
        date(transaction_timestamp) as transaction_date,
        extract(year from transaction_timestamp) as year,
        extract(month from transaction_timestamp) as month

    from base

)

select * from enhanced