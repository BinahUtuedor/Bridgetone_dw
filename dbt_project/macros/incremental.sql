{% macro incremental_filter(timestamp_column) %}

-- Applies incremental filtering when running in incremental mode
-- Only selects rows where timestamp_column is greater than the max value
-- already present in the target table ({{ this }})

{% if execute and is_incremental() %}

where {{ timestamp_column }} >
(
    select max({{ timestamp_column }})
    from {{ this }}
)

{% endif %}

{% endmacro %}