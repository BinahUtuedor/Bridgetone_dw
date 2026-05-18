{% macro standardize_payment_method(column_name) %}

case
    when lower(trim({{ column_name }})) in ('card', 'credit card')
        then 'card'

    when lower(trim({{ column_name }})) in ('cash')
        then 'cash'

    when lower(trim({{ column_name }})) in ('transfer', 'bank transfer')
        then 'transfer'

    else 'other'
end

{% endmacro %}