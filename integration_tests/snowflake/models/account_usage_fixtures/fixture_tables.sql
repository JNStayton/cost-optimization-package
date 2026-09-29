{{ config(alias='tables') }}
{#- Stand-in for ACCOUNT_USAGE.TABLES, one row per demo table (macros/demo_table_catalog.sql).
    The first branch takes its column types from the real view (and fails fast if
    Snowflake renames a column). -#}
select table_id, table_catalog, table_schema, table_name, table_type, row_count, clustering_key, is_transient, deleted
from snowflake.account_usage.tables where false
{% for t in demo_table_catalog() %}
union all
select {{ t.id }}, upper('{{ target.database }}'), upper('{{ target.schema }}'), '{{ t.name }}', 'BASE TABLE', {{ t.row_count }}, null, 'NO', null
{%- endfor %}
