{% macro safe_divide(numerator, denominator) %}

-- Safely divides two values while preventing division-by-zero errors
-- Returns NULL when denominator is 0
-- Otherwise returns numerator / denominator

case
    when {{ denominator }} = 0 then null
    else {{ numerator }} / {{ denominator }}
end

{% endmacro %}