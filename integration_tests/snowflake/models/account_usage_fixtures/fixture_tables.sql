{{ config(alias='tables') }}
{#- Stand-in for ACCOUNT_USAGE.TABLES. The first branch takes its column types from the
    real view (and fails fast if Snowflake renames a column). -#}
select table_id, table_catalog, table_schema, table_name, table_type, row_count, clustering_key, is_transient, deleted
from snowflake.account_usage.tables where false
union all
select 900001, upper('{{ target.database }}'), upper('{{ target.schema }}'), 'DEMO_EVENTS', 'BASE TABLE', 200000, null, 'NO', null
