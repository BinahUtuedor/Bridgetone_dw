{% macro null_if_blank(column_name) %}

case
    when trim({{ column_name }}) = '' then null
    else trim({{ column_name }})
end

{% endmacro %}