{#-
  End to end: each Snowflake gold view, built on the fixture-driven backlog, shows what the
  earlier slices put in. One row per check; returns only failed checks.
    - optimization_backlog: the 8 non-stable recommendations (the demoted clustering one
      is left out).
    - cost_savings_summary: actionable counts per domain (warehouse 2, materialization 4).
    - top_recommendations: FIXTURE_WH_BUSY's scale-up ranks first (tier 1; the pricier
      expensive-query signal is tier 2).
    - warehouse_optimizations: FIXTURE_WH_BUSY (scale-up, expensive query) and
      FIXTURE_WH_HEALTHY (expensive query).
    - dbt_model_optimizations: demo_orders, demo_logs, demo_infrequent_builds, demo_slow_view.
    - top_expensive_queries: demo_orders' query, with its incremental fix co-occurring.
    - top_queried_models: demo_events, 20 SELECTs.
    - user_level_cost_attribution: FIXTURE_BUILDER builds (68 CTAS), FIXTURE_ANALYST reads
      (98 SELECTs of project tables).
    - cross_domain_insights: demo_orders and demo_logs (expensive query + incremental).
    - top_spillage_models and ai_optimizations: empty (Standard edition path; no AI usage).
-#}
with checks as (
    select 'optimization_backlog rows' as check_name,
           (select count(*) from {{ ref('vw_snowflake__optimization_backlog') }})::varchar as produced, '8' as expected
    union all
    select 'optimization_backlog excludes demoted DEMO_EVENTS',
           (select count(*) from {{ ref('vw_snowflake__optimization_backlog') }}
            where endswith(upper(table_fqn), '.DEMO_EVENTS'))::varchar, '0'
    union all
    select 'cost_savings_summary counts',
           (select listagg(domain || '=' || total_recommendations, ',') within group (order by domain)
            from {{ ref('vw_snowflake__cost_savings_summary') }}), 'materialization=4,warehouse=2'
    union all
    select 'top_recommendations rank 1',
           (select listagg(signal_id || '@' || warehouse_name, ',') from {{ ref('vw_snowflake__top_recommendations') }}
            where priority_rank = 1), 'overload_scale_up_standard@FIXTURE_WH_BUSY'
    union all
    select 'warehouse_optimizations',
           (select listagg(warehouse_name || ':' || signal_id, ',') within group (order by warehouse_name, signal_id)
            from {{ ref('vw_snowflake__warehouse_optimizations') }}),
           'FIXTURE_WH_BUSY:expensive_query,FIXTURE_WH_BUSY:overload_scale_up_standard,FIXTURE_WH_HEALTHY:expensive_query'
    union all
    select 'dbt_model_optimizations models',
           (select listagg(model_name, ',') within group (order by model_name)
            from {{ ref('vw_snowflake__dbt_model_optimizations') }}),
           'demo_infrequent_builds,demo_logs,demo_orders,demo_slow_view'
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
           'FIXTURE_ANALYST:consumer:0/98,FIXTURE_BUILDER:builder:68/0'
    union all
    select 'cross_domain_insights',
           (select listagg(model_name || ':' || signal_count, ',') within group (order by model_name)
            from {{ ref('vw_snowflake__cross_domain_insights') }}),
           'demo_logs:2,demo_orders:2'
    union all
    select 'top_spillage_models rows',
           (select count(*) from {{ ref('vw_snowflake__top_spillage_models') }})::varchar, '0'
    union all
    select 'ai_optimizations rows',
           (select count(*) from {{ ref('vw_snowflake__ai_optimizations') }})::varchar, '0'
)

select * from checks where produced is distinct from expected
