{% macro audit_columns() %}

-- Standard audit columns for tracking record creation and update times
-- Both fields default to the current timestamp at query execution time
-- Commonly used in staging and fact tables for data lineage and freshness tracking

current_timestamp() as created_at,
current_timestamp() as updated_at

{% endmacro %}