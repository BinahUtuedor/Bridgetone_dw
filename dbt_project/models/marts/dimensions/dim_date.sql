{{ config(materialized='table') }}

with date_spine as (

    -- Native Snowflake date spine (reliable)
    select 
        dateadd(day, seq4(), '2020-01-01') as date_day
    from table(generator(rowcount => 4018))  -- ~11 years

)

select
    -- 🔑 Primary key (use this in fact tables)
    to_number(to_char(date_day, 'YYYYMMDD')) as date_key,

    -- Natural date (for readability)
    date_day as date,

    -- Standard attributes
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day,
    quarter(date_day) as quarter,

    -- Names (Power BI friendly sorting later)
    to_varchar(date_day, 'MMMM') as month_name,
    to_varchar(date_day, 'DY') as day_name,

    -- Week logic (ISO = Monday start, Power BI friendly)
    dayofweekiso(date_day) as day_of_week,

    case 
        when dayofweekiso(date_day) in (6,7) then true
        else false
    end as is_weekend,

    -- Quarter label
    case
        when month(date_day) in (1,2,3) then 'Q1'
        when month(date_day) in (4,5,6) then 'Q2'
        when month(date_day) in (7,8,9) then 'Q3'
        else 'Q4'
    end as quarter_name,

    -- 🔥 Power BI essentials
    to_char(date_day, 'YYYY-MM') as year_month,
    to_number(to_char(date_day, 'YYYYMM')) as year_month_key,

    -- Sorting helpers (CRITICAL for Power BI visuals)
    month(date_day) as month_number,
    dayofweekiso(date_day) as weekday_number,

    -- Flags used in reporting
    case when date_day = current_date() then true else false end as is_today,
    case when year(date_day) = year(current_date()) then true else false end as is_current_year

from date_spine