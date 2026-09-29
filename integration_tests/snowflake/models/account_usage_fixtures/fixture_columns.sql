{{ config(alias='columns') }}
{#- Stand-in for ACCOUNT_USAGE.COLUMNS, one row per demo table column
    (macros/demo_table_catalog.sql). Types come from the real view. -#}
select table_catalog, table_schema, table_name, column_name, ordinal_position, data_type, is_nullable,
       character_maximum_length, numeric_precision, deleted
from snowflake.account_usage.columns where false
{% for t in demo_table_catalog() %}
{%- for name, dtype in t.columns %}
union all
select upper('{{ target.database }}'), upper('{{ target.schema }}'), '{{ t.name }}', '{{ name }}', {{ loop.index }}, '{{ dtype }}', 'YES',
       {{ '16777216' if dtype == 'TEXT' else 'null' }}, {{ '38' if dtype == 'NUMBER' else 'null' }}, null
{%- endfor %}
{%- endfor %}
