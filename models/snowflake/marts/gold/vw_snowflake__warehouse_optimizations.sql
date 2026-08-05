{{
  config(
    materialized='view',
  )
}}

{#--
  Warehouse-level optimizations: one row per (warehouse, signal category).
  Config recs are standalone. Model-level signals (spillage, expensive queries)
  are aggregated with linked model context.

  Audience: Snowflake admins.
  Grain: one row per (warehouse_name, signal_id category).

  For conditional config (scale up, MCW): shows which models need fixing first
  and defers the warehouse action behind those model-level fixes.
--#}

with warehouse_config as (
    select warehouse_name, current_size, warehouse_category
    from {{ ref('int_snowflake__warehouse_config') }}
),

-- Config recs: already 1 per warehouse (from config fact)
config_recs as (
    select
        ar.warehouse_name,
        ar.signal_id,
        ar.priority_tier,
        ar.hierarchy_rank,
        ar.effort_category,
        ar.backlog_status,
        ar.recommendation,
        ar.recommendation_reason,
        ar.estimated_annual_cost_usd,
        ar.estimated_annual_savings_usd,
        ar.snowflake_ddl,
        null as affected_model_count,
        null as affected_models,
        ar.snapshot_date
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    where ar.domain = 'warehouse'
      and ar.signal_id in (
          'idle_reduce_auto_suspend', 'idle_switch_scaling_policy',
          'idle_reduce_max_clusters', 'idle_reduce_min_clusters',
          'idle_enable_mcw_bursty',
          'provisioning_enable_auto_resume', 'provisioning_increase_suspend',
          'provisioning_increase_suspend_300', 'provisioning_warm_cluster',
          'overload_switch_scaling_policy', 'spillage_scale_up',
          'overload_enable_mcw', 'overload_scale_up_standard',
          'overload_increase_clusters', 'overload_scale_up_large_mcw',
          'oversized_scale_down', 'oversized_disable_mcw',
          'overload_at_max_standard', 'idle_consolidate_standard',
          'idle_consolidate_underloaded', 'provisioning_gen2'
      )
      and ar.backlog_status in ('actionable', 'monitor')
),

-- Model-level signals: aggregate to 1 row per (warehouse, signal_id)
model_signals as (
    select
        ar.warehouse_name,
        ar.signal_id,
        min(ar.priority_tier) as priority_tier,
        min(ar.hierarchy_rank) as hierarchy_rank,
        max(ar.effort_category) as effort_category,
        max(ar.backlog_status) as backlog_status,
        max(ar.recommendation) as recommendation,
        count(distinct ar.node_id) as affected_model_count,
        listagg(distinct coalesce(ar.node_model_name, ar.model_name), ', ')
            within group (order by coalesce(ar.node_model_name, ar.model_name)) as affected_models,
        sum(ar.estimated_annual_cost_usd) as estimated_annual_cost_usd,
        sum(ar.estimated_annual_savings_usd) as estimated_annual_savings_usd,
        max(ar.snapshot_date) as snapshot_date
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    where ar.domain = 'warehouse'
      and ar.signal_id in ('spillage_moderate_worsening', 'spillage_moderate_stable', 'expensive_query_monitor', 'expensive_query_actionable')
      and ar.backlog_status in ('actionable', 'monitor')
      and ar.warehouse_name is not null
    group by ar.warehouse_name, ar.signal_id
),

combined as (
    select
        warehouse_name, signal_id, priority_tier, hierarchy_rank, effort_category,
        backlog_status, recommendation,
        case
            when signal_id like 'spillage%' and affected_model_count is null
                then recommendation_reason
            else recommendation_reason
        end as recommendation_reason,
        estimated_annual_cost_usd, estimated_annual_savings_usd, snowflake_ddl,
        affected_model_count, affected_models, snapshot_date
    from config_recs

    union all

    select
        warehouse_name, signal_id, priority_tier, hierarchy_rank, effort_category,
        backlog_status, recommendation,
        affected_model_count || ' model(s) affected: ' || affected_models
            || '. Resolve model-level optimizations (clustering/incremental) before applying warehouse config changes.' as recommendation_reason,
        estimated_annual_cost_usd, estimated_annual_savings_usd,
        null as snowflake_ddl,
        affected_model_count, affected_models, snapshot_date
    from model_signals
)

select
    c.warehouse_name,
    wc.current_size as warehouse_current_size,
    wc.warehouse_category,
    c.signal_id,
    c.priority_tier,
    c.effort_category,
    c.backlog_status,
    c.recommendation,
    c.recommendation_reason,
    c.estimated_annual_cost_usd,
    c.estimated_annual_savings_usd,
    c.snowflake_ddl,
    c.affected_model_count,
    c.affected_models,
    c.snapshot_date
from combined as c
left join warehouse_config as wc on wc.warehouse_name = c.warehouse_name
order by c.warehouse_name, c.hierarchy_rank, c.priority_tier
