{% macro validate_total_amount(quantity, unit_price, total_amount) %}

(
    {{ quantity }} * {{ unit_price }}
) = {{ total_amount }}

{% endmacro %}