{{ config(alias='columns') }}
{#- Stand-in for ACCOUNT_USAGE.COLUMNS for DEMO_EVENTS. Types come from the real view. -#}
select table_catalog, table_schema, table_name, column_name, ordinal_position, data_type, is_nullable,
       character_maximum_length, numeric_precision, deleted
from snowflake.account_usage.columns where false
{%- set cols = [('EVENT_ID', 1, 'NUMBER'), ('EVENT_DATE', 2, 'DATE'), ('CUSTOMER_ID', 3, 'NUMBER'),
                ('REGION', 4, 'TEXT'), ('IS_TEST', 5, 'BOOLEAN'), ('AMOUNT', 6, 'NUMBER')] %}
{% for name, pos, dtype in cols %}
union all
select upper('{{ target.database }}'), upper('{{ target.schema }}'), 'DEMO_EVENTS', '{{ name }}', {{ pos }}, '{{ dtype }}', 'YES',
       {{ '16777216' if dtype == 'TEXT' else 'null' }}, {{ '38' if dtype == 'NUMBER' else 'null' }}, null
{%- endfor %}
