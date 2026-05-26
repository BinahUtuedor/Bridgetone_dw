-- Replaces blank strings with NULL and trims whitespace
{% macro null_if_blank(column_name) %}

-- If value is empty string after trimming, convert to NULL
case
    when trim({{ column_name }}) = '' then null
    else trim({{ column_name }})
end

{% endmacro %}