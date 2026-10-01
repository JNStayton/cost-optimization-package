{#-
  Phase V: view chain evidence on the tables at the end of chains.
    - int_snowflake__table_upstream_views: demo_chain_table's builds recompute
      demo_chain_step (ephemeral), demo_chain_mid_view and demo_chain_base_view, nearest
      first; its recommended view is the chain's 'recommended' view in the materialization
      fact. demo_slow_view_rollup has one upstream view. Tables built from no views
      (demo_orders) have no row.
    - Enterprise edition (spillage): demo_chain_table spills (moderate), so it has two
      signals, spillage and view_chain, and appears in cross-domain insights with the
      chain as the headline action. Its spillage row lists the chain, and its warehouse's
      spillage group (FIXTURE_WH_HEALTHY) lists it among the models in view chains.
    - Standard edition: no spillage, so demo_chain_table has only the view_chain signal
      and isn't in cross-domain insights.
    - No other insight carries the view_chain signal (their models end no chains).
  Returns rows only on failure.
-#}
{%- set is_enterprise = var('snowflake_enterprise_edition', true) %}
with recommended as (
    select lower(model_name) as model_name
    from {{ ref('fct_snowflake__table_materialization_candidates') }}
    where chain_role = 'recommended' and startswith(lower(model_name), 'demo_chain_')
),

tuv as (
    select lower(table_model_name) as table_model_name, upstream_view_chain, upstream_view_count,
           lower(chain_recommended_view) as chain_recommended_view
    from {{ ref('int_snowflake__table_upstream_views') }}
),

checks as (
    select 'chain listed nearest first, ephemeral included' as check_name,
        (select upstream_view_chain || ' / ' || upstream_view_count from tuv where table_model_name = 'demo_chain_table')
            as produced,
        'demo_chain_step (ephemeral), demo_chain_mid_view (view), demo_chain_base_view (view) / 3' as expected
    union all select 'chain recommended view matches the materialization fact',
        (select chain_recommended_view from tuv where table_model_name = 'demo_chain_table'),
        (select model_name from recommended)
    union all select 'single-view chain',
        (select upstream_view_chain || ' / ' || chain_recommended_view from tuv where table_model_name = 'demo_slow_view_rollup'),
        'demo_slow_view (view) / demo_slow_view'
    union all select 'tables built from no views have no row',
        (select count(*) from tuv where table_model_name = 'demo_orders')::varchar, '0'
    union all select 'cross-domain insight for demo_chain_table',
        (select coalesce(max(array_to_string(array_sort(signals), ',') || ' | ' || recommended_action), 'none')
         from {{ ref('vw_snowflake__cross_domain_insights') }} where model_name = 'demo_chain_table'),
        {% if is_enterprise -%}
        'spillage,view_chain | Materialize ' || (select model_name from recommended)
            || ' as a table: this model''s builds recompute 3 upstream view(s), which adds to its spill'
        {%- else -%}
        'none'
        {%- endif %}
    union all select 'no other insight has a view_chain signal',
        (select count(*) from {{ ref('vw_snowflake__cross_domain_insights') }}
         where model_name != 'demo_chain_table' and array_contains('view_chain'::variant, signals))::varchar, '0'
    union all select 'spilling table shows its chain',
        (select coalesce(max(upstream_view_count || ' / ' || upstream_view_chain), 'none')
         from {{ ref('fct_snowflake__warehouse_performance_recommendations') }}
         where lower(model_name) = 'demo_chain_table'),
        {% if is_enterprise -%}
        '3 / demo_chain_step (ephemeral), demo_chain_mid_view (view), demo_chain_base_view (view)'
        {%- else -%}
        'none'
        {%- endif %}
    union all select 'top spillage model shows its chain',
        (select coalesce(max(upstream_view_count::varchar), 'none')
         from {{ ref('vw_snowflake__top_spillage_models') }} where lower(model_name) = 'demo_chain_table'),
        '{{ "3" if is_enterprise else "none" }}'
    union all select 'warehouse spillage group lists models in view chains',
        (select coalesce(max(affected_models_in_view_chains || ' | ' || left(view_chain_note, 20)), 'none')
         from {{ ref('vw_snowflake__warehouse_optimizations') }}
         where warehouse_name = 'FIXTURE_WH_HEALTHY' and signal_id = 'spillage'),
        '{{ "demo_chain_table (3 upstream view(s)) | 1 of 1 spilling mode" if is_enterprise else "none" }}'
)

select * from checks where produced is distinct from expected
