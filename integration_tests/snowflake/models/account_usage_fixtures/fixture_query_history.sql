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

{#-
  Clustering cases (fct_snowflake__table_clustering_candidates and its hooks):
    - 20 REAL filtered SELECTs against demo_events run while this model builds, and their
      real query IDs are recorded below. The extract_operator_evidence hook passes query IDs
      to GET_QUERY_OPERATOR_STATS, which rejects IDs that don't exist, so these must be real.
      Result caching is turned off first, since a cached result has no table scan to report.
    - 2 INSERTs (fake IDs are fine: the hook only analyzes SELECTs) give a read/write ratio > 1.
-#}
-- depends_on: {{ ref('demo_events') }}
{%- set clustering_qids = [] -%}
{%- if execute -%}
  {%- do run_query("alter session set use_cached_result = false") -%}
  {%- for i in range(20) -%}
    {%- do run_query("select count(*), sum(amount) from " ~ ref('demo_events') ~ " where region = 'EU' and event_date >= dateadd(day, -" ~ (i + 1) ~ ", current_date())") -%}
    {%- do clustering_qids.append(run_query("select last_query_id()").columns[0].values()[0]) -%}
  {%- endfor -%}
{%- endif %}

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

{%- set events_fqn = target.database ~ '.' ~ target.schema ~ '.demo_events' %}
{% for qid in clustering_qids %}
union all
select
    '{{ qid }}', dateadd(minute, -{{ loop.index }}, current_timestamp()), 'hash_events_{{ loop.index }}', 'phash_events_{{ loop.index }}',
    'FIXTURE_ANALYST', 'FIXTURE_REPORTER', 'FIXTURE_WH', 'X-Small', 5000, 52428800, 100, 0, 0, 'SELECT', 5000, 90, 100, 0, 0,
    'select count(*), sum(amount) from {{ events_fqn }} where region = ''EU'' and event_date >= current_date() - {{ loop.index }}',
    1, 'SUCCESS', 0
{%- endfor %}
{% for i in range(2) %}
union all
select
    'insert_events_{{ i }}', dateadd(hour, -{{ i + 3 }}, current_timestamp()), 'hash_events_insert', 'phash_events_insert',
    'FIXTURE_LOADER', 'FIXTURE_LOADER', 'FIXTURE_WH', 'X-Small', 3000, 1048576, 100, 0, 0, 'INSERT', 3000, 1, 1, 0, 0,
    'insert into {{ events_fqn }} select * from staging_events', 1, 'SUCCESS', 1000
{%- endfor %}

