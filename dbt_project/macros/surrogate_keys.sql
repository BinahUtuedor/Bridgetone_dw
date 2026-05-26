{% macro generate_surrogate_key(columns) %}

-- Generate a deterministic surrogate key by:
-- 1. Converting each column value to string
-- 2. Replacing null values with empty strings
-- 3. Concatenating values with a delimiter ('|')
-- 4. Applying MD5 hashing to create a consistent unique identifier
--
-- Example:
-- generate_surrogate_key(['customer_id', 'order_date'])
--
-- Input values:
-- customer_id = 123
-- order_date = '2026-05-26'
--
-- Concatenated value:
-- 123|2026-05-26
--
-- Output:
-- e.g. a1b2c3d4...

md5(
    {% for column in columns %}
        coalesce(cast({{ column }} as string), '')
        {% if not loop.last %} || '|' || {% endif %}
    {% endfor %}
)

{% endmacro %}