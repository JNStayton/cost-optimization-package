{{
  config(
    materialized='view',
  )
}}

{#--
  Warehouse-level optimizations: one row per warehouse with the top config
  recommendation (idle, overload, provisioning, oversized) and spillage as
  an amplifier signal.

  Audience: Snowflake admins.
  Grain: one row per warehouse.

  Expensive queries are surfaced separately in vw_snowflake__top_expensive_queries.
  Model-level spillage detail is in fct_snowflake__warehouse_performance_recommendations.
--#}

with config_recs as (
    select
        ar.warehouse_name,
        ar.effort_category,
        ar.backlog_status,
        ar.recommendation,
        ar.recommendation_reason,
        ar.estimated_annual_cost_usd,
        ar.estimated_annual_savings_usd,
        ar.score,
        ar.snowflake_ddl,
        ar.snapshot_date,
        -- Use the symptom directly from the config fact (passed via entity_name = warehouse_name)
        case
            when ar.recommendation like '%auto-suspend%' or ar.recommendation like '%idle%'
                or ar.recommendation like '%Consolidate%' or ar.recommendation like '%underloaded%'
                then 'idle_credit_consumption'
            when ar.recommendation like '%Scale up%' or ar.recommendation like '%cluster%'
                or ar.recommendation like '%multi-cluster%' or ar.recommendation like '%Split%'
                or ar.recommendation like '%queuing%'
                then 'query_overload'
            when ar.recommendation like '%cold start%' or ar.recommendation like '%provisioning%'
                or ar.recommendation like '%auto-resume%' or ar.recommendation like '%Gen2%'
                or ar.recommendation like '%warm%'
                then 'queued_provisioning'
            when ar.recommendation like '%Scale down%' or ar.recommendation like '%Disable%'
                or ar.recommendation like '%oversized%' or ar.recommendation like '%minimum size%'
                then 'oversized'
            else 'monitor'
        end as symptom,
        row_number() over (
            partition by ar.warehouse_name
            order by ar.estimated_annual_savings_usd desc nulls last, ar.score desc
        ) as rn
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    where ar.domain = 'warehouse'
      and ar.effort_category = 'config_change'
      and ar.backlog_status in ('actionable', 'monitor')
),

spillage_summary as (
    select
        sp.warehouse_name,
        count(*) as models_with_spillage,
        round(sum(sp.total_gb_spilled_local), 2) as total_gb_spilled_local_30d,
        round(sum(sp.total_gb_spilled_remote), 2) as total_gb_spilled_remote_30d,
        max(case
            when sp.total_gb_spilled_remote > 0 then 'Remote spillage detected — critical'
            when sp.total_gb_spilled_local > 50 then 'Heavy local spillage — scale up recommended'
            when sp.total_gb_spilled_local > 10 then 'Moderate local spillage — monitor'
            else 'Light spillage'
        end) as spillage_signal
    from {{ ref('fct_snowflake__warehouse_performance_recommendations') }} as sp
    where sp.recommendation not like 'Not available%'
    group by sp.warehouse_name
),

warehouse_config as (
    select warehouse_name, current_size, warehouse_category
    from {{ ref('int_snowflake__warehouse_config') }}
)

select
    cr.warehouse_name,
    wc.current_size as warehouse_current_size,
    wc.warehouse_category,
    cr.symptom,
    cr.backlog_status,
    cr.recommendation,
    cr.recommendation_reason,
    cr.estimated_annual_cost_usd,
    cr.estimated_annual_savings_usd,
    cr.snowflake_ddl,
    -- Spillage amplifier
    coalesce(ss.models_with_spillage, 0) as models_with_spillage,
    coalesce(ss.total_gb_spilled_local_30d, 0) as total_gb_spilled_local_30d,
    coalesce(ss.total_gb_spilled_remote_30d, 0) as total_gb_spilled_remote_30d,
    ss.spillage_signal,
    cr.snapshot_date
from config_recs as cr
left join spillage_summary as ss on ss.warehouse_name = cr.warehouse_name
left join warehouse_config as wc on wc.warehouse_name = cr.warehouse_name
where cr.rn = 1
order by cr.estimated_annual_savings_usd desc nulls last, cr.score desc
