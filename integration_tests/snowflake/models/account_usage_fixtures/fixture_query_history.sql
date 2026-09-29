{{ config(alias='query_history') }}

{#-
  Stand-in for SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY with only the columns
  stg_snowflake__query_history reads. Times are relative to now, so the rows always
  fall inside the package's lookback windows.

  Materialization cases (fct_snowflake__table_materialization_candidates):
    - demo_slow_view:  60 SELECTs at 45 s each (> 50 queries, > 10 s average)
                       → "Materialize as TABLE" (slow-query rule)
    - demo_quiet_view: 15 SELECTs at 1 s each (>= 10 queries, below thresholds) → "Monitor"
    - demo_rare_view:   3 SELECTs (below the 10-query minimum) → not listed
  Scans are 1 MB so the large-scan rule never applies.
  Demo views are named as plain text, not with ref(): a ref() would make this fixture
  depend on them, and the package would then (correctly) see each view as feeding a table.
-#}

{%- set cases = [
    {'view': 'demo_slow_view',  'queries': 60, 'elapsed_ms': 45000},
    {'view': 'demo_quiet_view', 'queries': 15, 'elapsed_ms': 1000},
    {'view': 'demo_rare_view',  'queries': 3,  'elapsed_ms': 1000},
] -%}

{% for c in cases %}
select
    '{{ c.view }}_q' || seq4()                                   as query_id,
    dateadd(hour, -(seq4() + 1), current_timestamp())            as start_time,
    'hash_{{ c.view }}'                                          as query_hash,
    'phash_{{ c.view }}'                                         as query_parameterized_hash,
    'FIXTURE_ANALYST'                                            as user_name,
    'FIXTURE_REPORTER'                                           as role_name,
    'FIXTURE_WH'                                                 as warehouse_name,
    'X-Small'                                                    as warehouse_size,
    {{ c.elapsed_ms }}                                           as total_elapsed_time,
    1048576                                                      as bytes_scanned,
    100                                                          as query_load_percent,
    0                                                            as queued_overload_time,
    0                                                            as queued_provisioning_time,
    'SELECT'                                                     as query_type,
    {{ c.elapsed_ms }}                                           as execution_time,
    10                                                           as partitions_scanned,
    10                                                           as partitions_total,
    0                                                            as bytes_spilled_to_local_storage,
    0                                                            as bytes_spilled_to_remote_storage,
    'select * from {{ target.database }}.{{ target.schema }}.{{ c.view }}' as query_text,
    1                                                            as session_id,
    'SUCCESS'                                                    as execution_status,
    0                                                            as rows_inserted
from table(generator(rowcount => {{ c.queries }}))
{% if not loop.last %}union all{% endif %}
{% endfor %}
