{% macro generate_schema_name(custom_schema_name, node) %}

    -- If no custom schema is defined in the model config,
    -- use the schema from the active target (profiles.yml)
    {%- if custom_schema_name is none -%}
        {{ target.schema }}

    -- If a custom schema is provided,
    -- use it directly instead of the default dbt schema naming pattern
    {%- else -%}
        {{ custom_schema_name }}

    {%- endif -%}

{% endmacro %}