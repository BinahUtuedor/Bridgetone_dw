{% macro calculate_avg_order_value(sales, transactions) %}

{{ safe_divide(sales, transactions) }}

{% endmacro %}