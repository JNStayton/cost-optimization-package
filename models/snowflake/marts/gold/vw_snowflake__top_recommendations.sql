{{
  config(
    materialized='view',
  )
}}

{#--
  Top recommendations across all domains — the executive action list.
  Shows only P1 (actionable now) signals, deduplicated per entity
  (highest savings as representative). Includes related_signals_count
  to indicate when an entity has additional optimizations available.

  This is the flagship gold-layer view.
--#}

with p1_recs as (
    select
        ar.*,
        row_number() over (
            partition by ar.dedup_key
            order by ar.estimated_annual_savings_usd desc nulls last, ar.score desc
        ) as entity_rank,
        count(*) over (partition by ar.dedup_key) as related_signals_count
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    where ar.priority_tier = 1
      and ar.backlog_status = 'actionable'
)

select
    dense_rank() over (order by estimated_annual_savings_usd desc nulls last, score desc) as priority_rank,
    domain,
    signal_id,
    node_id,
    coalesce(node_model_name, model_name) as model_name,
    node_project_name as project_name,
    table_fqn,
    warehouse_name,
    recommendation,
    recommendation_reason,
    estimated_annual_cost_usd,
    estimated_annual_savings_usd,
    snowflake_ddl,
    dbt_model_config,
    related_signals_count,
    snapshot_date
from p1_recs
where entity_rank = 1
order by priority_rank
