{% macro generate_date_spine(start_date, num_days) %}

select
    dateadd(day, seq4(), '{{ start_date }}') as date_day
from table(generator(rowcount => {{ num_days }}))

{% endmacro %}