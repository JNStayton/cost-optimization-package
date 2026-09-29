{{ config(alias='table_storage_metrics') }}
{#- Stand-in for ACCOUNT_USAGE.TABLE_STORAGE_METRICS, one row per demo table
    (macros/demo_table_catalog.sql). Types come from the real view. -#}
select id, table_catalog, table_schema, table_name, active_bytes, time_travel_bytes, failsafe_bytes, retained_for_clone_bytes, deleted
from snowflake.account_usage.table_storage_metrics where false
{% for t in demo_table_catalog() %}
union all
select {{ t.id }}, upper('{{ target.database }}'), upper('{{ target.schema }}'), '{{ t.name }}', {{ t.size_gb }} * power(1024, 3), 0, 0, 0, false
{%- endfor %}
