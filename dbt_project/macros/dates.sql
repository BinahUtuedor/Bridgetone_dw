{% macro generate_date_spine(start_date, num_days) %}

-- Generate a continuous date spine starting from a given date
-- and extending for a specified number of days.
--
-- This macro is commonly used to:
--   - Build calendar/date dimension scaffolding
--   - Ensure complete time series coverage (even for missing dates)
--   - Support reporting and time-based aggregations
--
-- Parameters:
--   start_date : The starting date of the spine (inclusive)
--   num_days   : Number of consecutive days to generate
--
-- Implementation details:
--   - Uses a Snowflake generator function to create a sequence of rows
--   - seq4() produces a zero-based incrementing integer
--   - dateadd(day, seq4(), start_date) shifts the start date forward
--     for each generated row

select
    dateadd(day, seq4(), '{{ start_date }}') as date_day
from table(generator(rowcount => {{ num_days }}))

{% endmacro %}