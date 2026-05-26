{% macro calculate_avg_order_value(sales, transactions) %}

-- Calculates average order value by dividing total sales by number of transactions
-- Uses safe_divide to prevent division by zero errors

{{ safe_divide(sales, transactions) }}

{% endmacro %}