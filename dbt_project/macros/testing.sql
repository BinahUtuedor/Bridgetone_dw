{% macro validate_total_amount(quantity, unit_price, total_amount) %}

-- Validates that quantity * unit_price equals total_amount
(
    {{ quantity }} * {{ unit_price }}
) = {{ total_amount }}

{% endmacro %}