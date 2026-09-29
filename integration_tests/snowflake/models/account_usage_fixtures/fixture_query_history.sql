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

{#-
  Incremental cases (fct_snowflake__incremental_materialization_candidates and
  fct_snowflake__incremental_config_recommendations): daily CREATE TABLE AS SELECT builds
  of the demo tables, 400 s each, run by FIXTURE_BUILDER from dbt session 110 on
  FIXTURE_WH_BUILD, each tagged with its model's node_id (so the gold user attribution
  sees them as builds). Row counts per build set the rebuild redundancy.
    - demo_orders, demo_sessions, demo_logs: 14 daily builds, +5,000 rows a day on
      1,000,000 (~99.5% unchanged per rebuild) → "Strong Candidate".
    - demo_fast_growth: 14 daily builds, rows triple each day (33% unchanged) → "Low ROI".
    - demo_new_table: 2 builds → "Insufficient History" (and, too thin for an ROI tier,
      left out of the config recommendations).
    - demo_infrequent_builds: 10 builds in the 60-day window (< 0.2 a day), so confidence
      starts at 50 ('investigate'); the probe confirms its key → 60 ('actionable_review').
-#}
{%- set build_cases = [
    {'table': 'demo_orders',      'days': 14, 'growth': 'linear'},
    {'table': 'demo_sessions',    'days': 14, 'growth': 'linear'},
    {'table': 'demo_logs',        'days': 14, 'growth': 'linear'},
    {'table': 'demo_fast_growth', 'days': 14, 'growth': 'triple'},
    {'table': 'demo_new_table',   'days': 2,  'growth': 'linear'},
    {'table': 'demo_infrequent_builds', 'days': 10, 'growth': 'linear'},
] -%}

{#-
  Warehouse cases (fct_snowflake__warehouse_config_recommendations and
  fct_snowflake__expensive_query_recommendations): 4 queries a day for 6 days on each
  fixture warehouse, from that warehouse's dbt session (macros/demo_warehouse_catalog.sql).
  One query hash per warehouse; a dbt query comment carries node_id where one is set.
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
{% for b in build_cases %}
{%- set fqn = target.database ~ '.' ~ target.schema ~ '.' ~ b.table %}
{%- for n in range(b.days) %}
{%- set rows = (1000 * 3 ** n) if b.growth == 'triple' else (1000000 + 5000 * n) %}
union all
select
    'build_{{ b.table }}_{{ n }}', dateadd(hour, -1, dateadd(day, -{{ b.days - n }}, current_timestamp())),
    'hash_build_{{ b.table }}', 'phash_build_{{ b.table }}',
    'FIXTURE_BUILDER', 'FIXTURE_TRANSFORMER', 'FIXTURE_WH_BUILD', 'X-Small', 400000, 1073741824, 100, 0, 0,
    'CREATE_TABLE_AS_SELECT', 400000, 100, 100, 0, 0,
    '/* {"app": "dbt", "node_id": "model.cost_optimization_integration_tests.{{ b.table }}"} */ '
        || 'create or replace transient table {{ fqn }} as (select * from upstream)', 110, 'SUCCESS', {{ rows }}
{%- endfor %}
{%- endfor %}
{% for w in demo_warehouse_catalog() %}
{%- for d in range(1, 7) %}
{%- for i in range(4) %}
union all
select
    'wh_{{ w.name | lower }}_{{ d }}_{{ i }}',
    dateadd(minute, {{ 5 + i * 10 }}, date_trunc('hour', dateadd(day, -{{ d }}, current_timestamp()))),
    'hash_{{ w.name | lower }}', 'phash_{{ w.name | lower }}',
    'FIXTURE_DBT', 'FIXTURE_TRANSFORMER', '{{ w.name }}', '{{ w.qh_size }}',
    {{ w.elapsed }}, 1048576, {{ w.load }}, {{ w.overload }}, {{ w.provisioning }}, 'SELECT', {{ w.exec }}, 1, 1, 0, 0,
    '{% if w.node_id %}/* {"app": "dbt", "node_id": "{{ w.node_id }}"} */ {% endif %}select 1',
    {{ w.session_id }}, 'SUCCESS', 0
{%- endfor %}
{%- endfor %}
{%- endfor %}

