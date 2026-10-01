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
  demo_slow_view also feeds a table, demo_slow_view_rollup (see the rows at the end):
    - 10 dbt CTAS builds of the rollup in the last 14 days, each re-running the view.
    - 1 non-dbt CTAS of a same-named table in another schema (must not count).
    - 4 dbt CREATE VIEW runs of demo_slow_view (after materializing, 4 table builds).
    So its cost covers 60 reads + 10 downstream builds = 70 runs of the view, and its
    savings 70 - 4 = 66.
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
    - 3 REAL filtered SELECTs on daily_demo_events, a child table of demo_events, more recent
      than the reads above, so the hook (10 queries per table) analyzes 3 child queries and
      7 demo_events reads. The child filters on EVENT_DATE must not count for demo_events:
      EVENT_DATE has 7 filtering queries out of 7 analyzed, not 10 of 7.
    - The reads also filter EVENT_ID, which must not count as a filter on ID.
    - The SELECTs take 1 s each, so the clustering recommendation's savings stay under the
      $1 min_annual_savings_usd floor (about $0.50/yr): the gold slice's "pennies demoted" case.
-#}
-- depends_on: {{ ref('demo_events') }}
-- depends_on: {{ ref('daily_demo_events') }}
{%- set clustering_qids = [] -%}
{%- set child_qids = [] -%}
{%- if execute -%}
  {%- do run_query("alter session set use_cached_result = false") -%}
  {%- for i in range(20) -%}
    {%- do run_query("select count(*), sum(amount) from " ~ ref('demo_events') ~ " where region = 'EU' and event_id <> 12345 and event_date >= dateadd(day, -" ~ (i + 1) ~ ", current_date())") -%}
    {%- do clustering_qids.append(run_query("select last_query_id()").columns[0].values()[0]) -%}
  {%- endfor -%}
  {%- for i in range(3) -%}
    {%- do run_query("select sum(events) from " ~ ref('daily_demo_events') ~ " where event_date >= dateadd(day, -" ~ (i + 2) ~ ", current_date())") -%}
    {%- do child_qids.append(run_query("select last_query_id()").columns[0].values()[0]) -%}
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

-- Typed first branch: every column takes the real view's type, and the build fails fast if
-- Snowflake renames one. Without it, columns would take narrow types from the literals
-- below (NUMBER(1,0) for a column that's always 0), and unit tests that read these types
-- would reject realistic values.
select
    query_id, start_time, query_hash, query_parameterized_hash, user_name, role_name,
    warehouse_name, warehouse_size, total_elapsed_time, bytes_scanned, query_load_percent,
    queued_overload_time, queued_provisioning_time, query_type, execution_time,
    partitions_scanned, partitions_total, bytes_spilled_to_local_storage,
    bytes_spilled_to_remote_storage, query_text, session_id, execution_status, rows_inserted
from snowflake.account_usage.query_history where false
union all
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
    'FIXTURE_ANALYST', 'FIXTURE_REPORTER', 'FIXTURE_WH', 'X-Small', 1000, 52428800, 100, 0, 0, 'SELECT', 1000, 90, 100, 0, 0,
    'select count(*), sum(amount) from {{ events_fqn }} where region = ''EU'' and event_id <> 12345 and event_date >= current_date() - {{ loop.index }}',
    1, 'SUCCESS', 0
{%- endfor %}
{%- set daily_fqn = target.database ~ '.' ~ target.schema ~ '.daily_demo_events' %}
{% for qid in child_qids %}
union all
select
    '{{ qid }}', dateadd(second, -{{ 5 + loop.index * 5 }}, current_timestamp()), 'hash_daily_{{ loop.index }}', 'phash_daily_{{ loop.index }}',
    'FIXTURE_ANALYST', 'FIXTURE_REPORTER', 'FIXTURE_WH', 'X-Small', 500, 1048576, 100, 0, 0, 'SELECT', 500, 1, 1, 0, 0,
    'select sum(events) from {{ daily_fqn }} where event_date >= current_date() - {{ loop.index + 1 }}',
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
{#- Target names in the dbt comment, for relation history: demo_orders is built under
    both 'default' and 'dev' into the same schema (one deployment, two targets, as a dbt
    platform Studio session records); demo_logs only under 'dev'. -#}
{%- set target_json = ', "target_name": "' ~ ('dev' if n % 2 == 1 else 'default') ~ '"' if b.table == 'demo_orders'
                      else (', "target_name": "dev"' if b.table == 'demo_logs' else '') %}
union all
select
    'build_{{ b.table }}_{{ n }}', dateadd(hour, -1, dateadd(day, -{{ b.days - n }}, current_timestamp())),
    'hash_build_{{ b.table }}', 'phash_build_{{ b.table }}',
    'FIXTURE_BUILDER', 'FIXTURE_TRANSFORMER', 'FIXTURE_WH_BUILD', 'X-Small', 400000, 1073741824, 100, 0, 0,
    'CREATE_TABLE_AS_SELECT', 400000, 100, 100, 0, 0,
    '/* {"app": "dbt", "node_id": "model.cost_optimization_integration_tests.{{ b.table }}"{{ target_json }}} */ '
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
{%- set rollup_fqn = target.database ~ '.' ~ target.schema ~ '.demo_slow_view_rollup' %}
{%- set slow_view_fqn = target.database ~ '.' ~ target.schema ~ '.demo_slow_view' %}
{% for n in range(10) %}
union all
select
    'rollup_build_{{ n }}', dateadd(hour, -(6 + {{ n }} * 24), current_timestamp()),
    'hash_rollup_build', 'phash_rollup_build',
    'FIXTURE_BUILDER', 'FIXTURE_TRANSFORMER', 'FIXTURE_WH_BUILD', 'X-Small', 60000, 1048576, 100, 0, 0,
    'CREATE_TABLE_AS_SELECT', 60000, 1, 1, 0, 0,
    '/* {"app": "dbt", "node_id": "model.cost_optimization_integration_tests.demo_slow_view_rollup"} */ '
        || 'create or replace transient table {{ rollup_fqn }} as (select * from {{ slow_view_fqn }})',
    110, 'SUCCESS', 60
{%- endfor %}
union all
select
    'rollup_decoy', dateadd(hour, -7, current_timestamp()), 'hash_rollup_decoy', 'phash_rollup_decoy',
    'FIXTURE_ANALYST', 'FIXTURE_REPORTER', 'FIXTURE_WH', 'X-Small', 60000, 1048576, 100, 0, 0,
    'CREATE_TABLE_AS_SELECT', 60000, 1, 1, 0, 0,
    'create table OTHER_DB.OTHER_SCHEMA.DEMO_SLOW_VIEW_ROLLUP as (select 1 as id)', 1, 'SUCCESS', 1
{% for n in range(4) %}
union all
select
    'slow_view_create_{{ n }}', dateadd(hour, -(5 + {{ n }} * 72), current_timestamp()),
    'hash_slow_view_create', 'phash_slow_view_create',
    'FIXTURE_BUILDER', 'FIXTURE_TRANSFORMER', 'FIXTURE_WH_BUILD', 'X-Small', 500, 0, 100, 0, 0,
    'CREATE_VIEW', 500, 0, 0, 0, 0,
    '/* {"app": "dbt", "node_id": "model.cost_optimization_integration_tests.demo_slow_view"} */ '
        || 'create or replace view {{ slow_view_fqn }} as (select 1 as id)',
    110, 'SUCCESS', 0
{%- endfor %}
{#-
  Spillage cases (fct_snowflake__warehouse_performance_recommendations, Enterprise
  edition): dbt builds of six demo tables that spill, 60 s each, tagged with the model's
  node_id but run from a non-dbt session so they don't change the warehouse, expensive
  query or user attribution results. Spill is in GB (local, remote), days ago.
-#}
{%- set gb = 1073741824 %}
{%- set spill_builds = [
    ('demo_spill_remote',      'FIXTURE_WH_IDLE',     2,  49,  2),
    ('demo_spill_worsening',   'FIXTURE_WH_IDLE',     3,  49,  0),
    ('demo_spill_steady',      'FIXTURE_WH_IDLE',     20, 2,   0),
    ('demo_spill_steady',      'FIXTURE_WH_IDLE',     5,  2,   0),
    ('demo_spill_heavy_small', 'FIXTURE_WH_COLD',     2,  60,  0),
    ('demo_spill_heavy_large', 'FIXTURE_WH_BUSY_2XL', 2,  60,  0),
    ('demo_spill_minor',       'FIXTURE_WH_HEALTHY',  2,  0.5, 0),
] %}
{% for tbl, wh, days_ago, local_gb, remote_gb in spill_builds %}
union all
select
    'spill_{{ tbl }}_{{ days_ago }}', dateadd(hour, -3, dateadd(day, -{{ days_ago }}, current_timestamp())),
    'hash_spill_{{ tbl }}', 'phash_spill_{{ tbl }}',
    'FIXTURE_BUILDER', 'FIXTURE_TRANSFORMER', '{{ wh }}', 'Small', 60000, 1048576, 100, 0, 0,
    'CREATE_TABLE_AS_SELECT', 60000, 1, 1, {{ (local_gb * gb) | int }}, {{ (remote_gb * gb) | int }},
    '/* {"app": "dbt", "node_id": "model.cost_optimization_integration_tests.{{ tbl }}"} */ '
        || 'create or replace transient table {{ target.database }}.{{ target.schema }}.{{ tbl }} as (select 1 as id)',
    1, 'SUCCESS', 1000
{%- endfor %}
{#-
  Relation history: a second deployment of demo_orders, in <schema>_deploy under target
  'prod' with a dbt platform environment id. Run from a non-dbt session so it only
  feeds relation history (not the warehouse, expensive query or user attribution
  results).
-#}
{% for n in range(3) %}
union all
select
    'deploy_demo_orders_{{ n }}', dateadd(hour, -(8 + {{ n }} * 24), current_timestamp()),
    'hash_deploy_orders', 'phash_deploy_orders',
    'FIXTURE_BUILDER', 'FIXTURE_TRANSFORMER', 'FIXTURE_WH_BUILD', 'X-Small', 400000, 1048576, 100, 0, 0,
    'CREATE_TABLE_AS_SELECT', 400000, 1, 1, 0, 0,
    '/* {"app": "dbt", "node_id": "model.cost_optimization_integration_tests.demo_orders", "target_name": "prod", "dbt_cloud_environment_id": "12345"} */ '
        || 'create or replace transient table {{ target.database }}.{{ target.schema }}_deploy.demo_orders as (select * from upstream)',
    1, 'SUCCESS', 1000000
{%- endfor %}

