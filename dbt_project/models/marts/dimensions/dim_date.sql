-- Creates a reusable calendar/date dimension table for analytics and reporting.
-- This model generates a continuous date spine and enriches it with
-- standard date attributes, reporting labels, and Power BI-friendly
-- sorting keys and flags.

{{ config(materialized='table') }}

with date_spine as (

    -- Generate a continuous sequence of dates beginning on 2020-01-01.
    -- Snowflake's generator function creates rows and seq4() increments
    -- by one for each day in the range (~11 years of coverage).
    select 
        dateadd(day, seq4(), '2020-01-01') as date_day
    from table(generator(rowcount => 4018))

)

select
    -- Surrogate date key in YYYYMMDD format.
    -- Intended for joining dimension and fact tables.
    to_number(to_char(date_day, 'YYYYMMDD')) as date_key,

    -- Human-readable calendar date.
    date_day as date,

    -- Core date components for filtering and grouping.
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day,
    quarter(date_day) as quarter,

    -- Display-friendly month and weekday names.
    -- Sort using month_number and weekday_number in BI tools.
    to_varchar(date_day, 'MMMM') as month_name,
    to_varchar(date_day, 'DY') as day_name,

    -- ISO weekday numbering (Monday = 1, Sunday = 7).
    dayofweekiso(date_day) as day_of_week,

    -- Flag weekends for reporting and business logic.
    case 
        when dayofweekiso(date_day) in (6,7) then true
        else false
    end as is_weekend,

    -- Quarter label for reporting visuals.
    case
        when month(date_day) in (1,2,3) then 'Q1'
        when month(date_day) in (4,5,6) then 'Q2'
        when month(date_day) in (7,8,9) then 'Q3'
        else 'Q4'
    end as quarter_name,

    -- Common reporting fields used in time-series analysis.
    to_char(date_day, 'YYYY-MM') as year_month,
    to_number(to_char(date_day, 'YYYYMM')) as year_month_key,

    -- Numeric sort columns to maintain proper ordering in BI tools.
    month(date_day) as month_number,
    dayofweekiso(date_day) as weekday_number,

    -- Dynamic flags for current-period reporting.
    case when date_day = current_date() then true else false end as is_today,
    case when year(date_day) = year(current_date()) then true else false end as is_current_year

from date_spine