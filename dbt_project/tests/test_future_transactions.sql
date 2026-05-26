-- Select all sales records from the fact table
select f.*

from {{ ref('fct_store_sales') }} f

-- Join with the date dimension to enrich/filter by calendar attributes
join {{ ref('dim_date') }} d
    on f.date_key = d.date_key

-- Filter to only include records where the date is in the future
-- (useful for testing data integrity or ensuring no future-dated facts exist)
where d.date > current_date()