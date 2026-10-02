{#-
  End to end: each Snowflake gold view, built on the fixture-driven backlog, shows what the
  earlier slices put in. One row per check; returns only failed checks.
    - all_recommendations: no recommendation appears twice (the integration project keeps a
      stale clustering snapshot beside the current one; only the latest may be read).
    - optimization_backlog: the 18 non-stable recommendations (the demoted clustering one
      is left out), including both view chain views (one actionable, its alternative
      monitor).
    - cost_savings_summary: actionable counts per domain (warehouse 8, materialization 5:
      only the recommended view of the chain, not its alternative).
    - Job-level spillage (both editions) adds 5 backlog rows: routing for job 7001's two
      models and job 7002's size-up (actionable, 3 more warehouse rows), and job 7003's
      routing (monitor). The routed models show in dbt_model_optimizations, and the job
      size-up in warehouse_optimizations (FIXTURE_WH_JOBS).
    - top_recommendations: FIXTURE_WH_BUSY's scale-up ranks first (tier 1; the pricier
      expensive-query signal is tier 2).
    - warehouse_optimizations: FIXTURE_WH_BUSY (scale-up, expensive query),
      FIXTURE_WH_HEALTHY (expensive query), and IDLE, COLD and BUSY_2XL (their config
      recommendations, in scope because the spillage slice's dbt builds run on them).
    - dbt_model_optimizations: demo_orders, demo_logs, demo_infrequent_builds, demo_slow_view,
      and the chain's recommended view (by role: real probe times pick which view), whose
      recompute cost comes from the probe.
    - top_expensive_queries: demo_orders' query, with its incremental fix co-occurring.
    - top_queried_models: demo_events, 20 SELECTs.
    - user_level_cost_attribution: FIXTURE_BUILDER builds (78 CTAS: 68 of the incremental
      slice's tables + 10 of demo_slow_view_rollup), FIXTURE_ANALYST reads (451 SELECTs of
      project tables, including 3 of daily_demo_events and 350 of the chain views). Credits:
      the builder's are all elapsed x list rate at the X-Small fallback (68 x 400 s + 10 x
      60 s = 7.7222, credits_from_attribution false); the analyst's 60 demo_slow_view reads
      use QUERY_ATTRIBUTION_HISTORY (60 x 0.001) and the other 391 reads the list rate
      (38 s + 3 x 0.5 s + 350 x 1 s, / 3600), 0.1682 in total, attribution true.
    - cross_domain_insights: demo_orders and demo_logs (expensive query + incremental).
    - top_spillage_models and ai_optimizations: empty (Standard edition path; no AI usage).
    - The clustering recommendation's config uses the suggested key (EVENT_DATE, REGION),
      not the table's existing clustering key; warehouse rows have no deployed_relation_count.
-#}
{#- FIXTURE_WH_BUSY's config signal depends on the edition (multi-cluster is Enterprise). -#}
{%- set is_enterprise = var('snowflake_enterprise_edition', true) %}
{%- set busy_signal = 'overload_enable_mcw' if is_enterprise else 'overload_scale_up_standard' %}
{#- Enterprise edition adds spillage (per-table performance recommendations): 7 more
    non-stable backlog rows (SQL refactor and the three scale-ups actionable, since their
    savings are null and the floor doesn't demote them; worsening, steady and the chain
    table monitor), four more actionable warehouse rows, spillage groups on BUSY,
    BUSY_2XL, IDLE and HEALTHY, 7 spilling models, the bursty multi-cluster recommendation
    (actionable with null savings: one more backlog row and actionable warehouse row), and demo_chain_table in cross-domain
    insights (spillage + view_chain). -#}
with checks as (
    select 'all_recommendations has no duplicate recommendations' as check_name,
           (select count(*) - count(distinct domain || '|' || signal_id || '|' || entity_name)
            from {{ ref('int_snowflake__all_recommendations') }})::varchar as produced, '0' as expected
    union all
    select 'optimization_backlog rows',
           (select count(*) from {{ ref('vw_snowflake__optimization_backlog') }})::varchar as produced,
           '{{ 26 if is_enterprise else 18 }}' as expected
    union all
    select 'optimization_backlog excludes demoted DEMO_EVENTS',
           (select count(*) from {{ ref('vw_snowflake__optimization_backlog') }}
            where endswith(upper(table_fqn), '.DEMO_EVENTS'))::varchar, '0'
    union all
    select 'cost_savings_summary counts',
           (select listagg(domain || '=' || total_recommendations, ',') within group (order by domain)
            from {{ ref('vw_snowflake__cost_savings_summary') }}), 'materialization=5,warehouse={{ 13 if is_enterprise else 8 }}'
    union all
    select 'top_recommendations rank 1',
           (select listagg(signal_id || '@' || warehouse_name, ',') from {{ ref('vw_snowflake__top_recommendations') }}
            where priority_rank = 1), '{{ busy_signal }}@FIXTURE_WH_BUSY'
    union all
    select 'warehouse_optimizations',
           (select listagg(warehouse_name || ':' || signal_id, ',') within group (order by warehouse_name, signal_id)
            from {{ ref('vw_snowflake__warehouse_optimizations') }}),
           '{{ "FIXTURE_WH_BURSTY:idle_enable_mcw_bursty," if is_enterprise else "" }}'
           || 'FIXTURE_WH_BUSY:expensive_query,FIXTURE_WH_BUSY:{{ busy_signal }},'
           || '{{ "FIXTURE_WH_BUSY:spillage," if is_enterprise else "" }}'
           || 'FIXTURE_WH_BUSY_2XL:{{ busy_signal }},'
           || '{{ "FIXTURE_WH_BUSY_2XL:spillage," if is_enterprise else "" }}'
           || 'FIXTURE_WH_COLD:provisioning_gen2,FIXTURE_WH_HEALTHY:expensive_query,'
           || '{{ "FIXTURE_WH_HEALTHY:spillage," if is_enterprise else "" }}'
           || 'FIXTURE_WH_IDLE:idle_reduce_auto_suspend'
           || '{{ ",FIXTURE_WH_IDLE:spillage" if is_enterprise else "" }}'
           || ',FIXTURE_WH_JOBS:spillage_job_scale_up'
    union all
    select 'dbt_model_optimizations models',
           (select listagg(iff(startswith(model_name, 'demo_chain_'),
                               'chain_' || chain_role || ':' || recompute_cost_source, model_name), ',')
                   within group (order by model_name)
            from {{ ref('vw_snowflake__dbt_model_optimizations') }}),
           'chain_recommended:probe,demo_infrequent_builds,demo_logs,demo_orders,demo_slow_view,job7001_m0,job7001_m1'
    union all
    select 'top_expensive_queries',
           (select listagg(model_name || ':' || co_occurring_fixes, ',') within group (order by model_name)
            from {{ ref('vw_snowflake__top_expensive_queries') }}),
           'demo_logs:incremental_config,demo_orders:incremental_config'
    union all
    select 'top_queried_models',
           (select listagg(model_name || ':' || total_selects_30d, ',') from {{ ref('vw_snowflake__top_queried_models') }}),
           'demo_events:20'
    union all
    select 'user_level_cost_attribution',
           (select listagg(user_name || ':' || user_category || ':' || build_query_count || '/' || consumption_query_count, ',')
                   within group (order by user_name)
            from {{ ref('vw_snowflake__user_level_cost_attribution') }}),
           'FIXTURE_ANALYST:consumer:0/451,FIXTURE_BUILDER:builder:78/0'
    union all
    select 'user_level_cost_attribution credits',
           (select listagg(user_name || ':' || (build_credits_30d + consumption_credits_30d)::number(10, 4)
                           || ':' || credits_from_attribution, ',') within group (order by user_name)
            from {{ ref('vw_snowflake__user_level_cost_attribution') }}),
           'FIXTURE_ANALYST:0.1682:true,FIXTURE_BUILDER:7.7222:false'
    union all
    select 'cross_domain_insights',
           (select listagg(model_name || ':' || signal_count, ',') within group (order by model_name)
            from {{ ref('vw_snowflake__cross_domain_insights') }}),
           '{{ "demo_chain_table:2," if is_enterprise else "" }}demo_logs:2,demo_orders:2'
    union all
    select 'top_spillage_models rows',
           (select count(*) from {{ ref('vw_snowflake__top_spillage_models') }})::varchar,
           '{{ 7 if is_enterprise else 0 }}'
    union all
    select 'ai_optimizations rows',
           (select count(*) from {{ ref('vw_snowflake__ai_optimizations') }})::varchar, '0'
    union all
    select 'clustering config from the suggested key',
           (select listagg(dbt_model_config, ',') from {{ ref('int_snowflake__all_recommendations') }}
            where domain = 'clustering'),
           '{{ "{{" }} config(cluster_by=[''EVENT_DATE'', ''REGION'']) {{ "}}" }}'
    union all
    select 'backlog rows with no model have no deployed_relation_count',
           (select count(*) from {{ ref('vw_snowflake__optimization_backlog') }}
            where node_id is null and deployed_relation_count is not null)::varchar, '0'
)

select * from checks where produced is distinct from expected
