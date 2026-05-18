{% macro incremental_filter(timestamp_column) %}

{% if is_incremental() %}

where {{ timestamp_column }} >
(
    select max({{ timestamp_column }})
    from {{ this }}
)

{% endif %}

{% endmacro %}