{% macro standardize_payment_method(column_name) %}

-- Standardize payment method values into a consistent set of categories:
--   - 'card'      → card, credit card
--   - 'cash'       → cash
--   - 'transfer'   → transfer, bank transfer
--   - 'other'      → any unrecognized or unexpected values
--
-- This macro:
-- 1. Normalizes text by trimming whitespace and converting to lowercase
-- 2. Maps known variants of payment methods into standard labels
-- 3. Groups unknown values into 'other' for data consistency

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