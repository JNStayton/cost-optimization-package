{#-
  What the ACCOUNT_USAGE fixtures report about each demo table: its size, row count and
  columns. The tables, table_storage_metrics and columns fixtures all read this list, so
  they stay consistent. Sizes and row counts are what the package sees; the real demo
  tables are small. Column names and types must match the real demo models, because
  hooks (probe_unique_key_candidates, refresh_column_cardinality) query the real tables.
-#}
{% macro demo_table_catalog() %}
  {{ return([
    {'name': 'DEMO_EVENTS', 'id': 900001, 'size_gb': 5, 'row_count': 200000,
     'columns': [('EVENT_ID', 'NUMBER'), ('EVENT_DATE', 'DATE'), ('CUSTOMER_ID', 'NUMBER'),
                 ('REGION', 'TEXT'), ('IS_TEST', 'BOOLEAN'), ('AMOUNT', 'NUMBER'), ('ID', 'NUMBER')]},
    {'name': 'DEMO_ORDERS', 'id': 900002, 'size_gb': 50, 'row_count': 1065000,
     'columns': [('ORDER_ID', 'NUMBER'), ('CUSTOMER_ID', 'NUMBER'), ('AMOUNT', 'NUMBER'),
                 ('UPDATED_AT', 'TIMESTAMP_NTZ')]},
    {'name': 'DEMO_SESSIONS', 'id': 900003, 'size_gb': 50, 'row_count': 1065000,
     'columns': [('SESSION_ID', 'NUMBER'), ('USER_ID', 'NUMBER'), ('STARTED_AT', 'TIMESTAMP_NTZ')]},
    {'name': 'DEMO_LOGS', 'id': 900004, 'size_gb': 50, 'row_count': 1065000,
     'columns': [('LOGGED_AT', 'TIMESTAMP_NTZ'), ('MESSAGE', 'TEXT')]},
    {'name': 'DEMO_FAST_GROWTH', 'id': 900005, 'size_gb': 50, 'row_count': 1594323000,
     'columns': [('ROW_ID', 'NUMBER'), ('LOADED_AT', 'TIMESTAMP_NTZ')]},
    {'name': 'DEMO_NEW_TABLE', 'id': 900006, 'size_gb': 50, 'row_count': 1005000,
     'columns': [('ROW_ID', 'NUMBER'), ('LOADED_AT', 'TIMESTAMP_NTZ')]},
    {'name': 'DEMO_INFREQUENT_BUILDS', 'id': 900007, 'size_gb': 50, 'row_count': 1045000,
     'columns': [('RECORD_ID', 'NUMBER'), ('UPDATED_AT', 'TIMESTAMP_NTZ')]},
    {'name': 'DEMO_SPILL_REMOTE', 'id': 900008, 'size_gb': 1, 'row_count': 1000,
     'columns': [('ID', 'NUMBER')]},
    {'name': 'DEMO_SPILL_WORSENING', 'id': 900009, 'size_gb': 1, 'row_count': 1000,
     'columns': [('ID', 'NUMBER')]},
    {'name': 'DEMO_SPILL_STEADY', 'id': 900010, 'size_gb': 1, 'row_count': 1000,
     'columns': [('ID', 'NUMBER')]},
    {'name': 'DEMO_SPILL_HEAVY_SMALL', 'id': 900011, 'size_gb': 1, 'row_count': 1000,
     'columns': [('ID', 'NUMBER')]},
    {'name': 'DEMO_SPILL_HEAVY_LARGE', 'id': 900012, 'size_gb': 1, 'row_count': 1000,
     'columns': [('ID', 'NUMBER')]},
    {'name': 'DEMO_SPILL_MINOR', 'id': 900013, 'size_gb': 1, 'row_count': 1000,
     'columns': [('ID', 'NUMBER')]},
  ]) }}
{% endmacro %}
