{{ config(alias='table_storage_metrics') }}
{#- Stand-in for ACCOUNT_USAGE.TABLE_STORAGE_METRICS. Types come from the real view. -#}
select id, table_catalog, table_schema, table_name, active_bytes, time_travel_bytes, failsafe_bytes, retained_for_clone_bytes, deleted
from snowflake.account_usage.table_storage_metrics where false
union all
select 900001, upper('{{ target.database }}'), upper('{{ target.schema }}'), 'DEMO_EVENTS', 5368709120, 0, 0, 0, false
