{% macro generate_surrogate_key(columns) %}

md5(
    {% for column in columns %}
        coalesce(cast({{ column }} as varchar), '')
        {% if not loop.last %} || '|' || {% endif %}
    {% endfor %}
)

{% endmacro %}