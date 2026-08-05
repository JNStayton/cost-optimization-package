{{
  config(
    materialized='view',
  )
}}

{#--
  Top expensive recurring queries within the project scope.
  Enriched with co-occurring optimization signals from other domains to show
  WHY each query is expensive and what the actionable fix is.

  A query with co-signals (clustering, incremental, materialization) is marked
  "Actionable" — the fix is known. A query with no co-signals is "Monitor" —
  it needs manual SQL investigation.

  Audience: dbt developers + Snowflake admins.
  Grain: one row per query_hash (deduplicated recurring query pattern).
  Ordered by estimated annual cost descending, limited to top 10.
--#}

with expensive as (
    select
        ar.warehouse_name,
        ar.node_id,
        coalesce(ar.node_model_name, ar.model_name) as model_name,
        ar.table_fqn,
        ar.entity_name as query_hash,
        ar.signal_id,
        ar.priority_tier,
        ar.estimated_annual_cost_usd,
        ar.estimated_annual_savings_usd,
        ar.score as total_credits_30d,
        ar.recommendation,
        ar.recommendation_reason,
        ar.backlog_status,
        ar.node_project_name as project_name,
        ar.snapshot_date
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    where ar.domain = 'warehouse'
      and ar.effort_category = 'sql_refactor'
      and ar.backlog_status in ('actionable', 'monitor')
),

-- Find co-occurring optimization signals for the same model
co_signals as (
    select
        ar.node_id,
        array_agg(distinct ar.signal_id) as optimization_signals,
        count(distinct ar.signal_id) as signal_count,
        min(ar.priority_tier) as best_priority_tier,
        listagg(distinct
            case
                when ar.signal_id like 'add_clustering%' then 'clustering'
                when ar.signal_id like 'convert_to_incremental%' then 'incremental'
                when ar.signal_id like 'apply_incremental%' then 'incremental_config'
                when ar.signal_id like 'materialize%' then 'materialization'
                when ar.signal_id like 'spillage%' then 'spillage'
                else ar.domain
            end, ' + '
        ) as fix_categories
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    where ar.domain in ('materialization', 'clustering')
      and ar.backlog_status = 'actionable'
      and ar.node_id is not null
    group by ar.node_id
)

select
    e.warehouse_name,
    e.node_id,
    e.model_name,
    e.table_fqn,
    e.query_hash,
    e.signal_id,
    -- Upgrade priority when co-signals exist (the query is expensive because of a fixable root cause)
    case
        when cs.signal_count > 0 then least(e.priority_tier, 2)
        else e.priority_tier
    end as priority_tier,
    e.estimated_annual_cost_usd,
    e.estimated_annual_savings_usd,
    e.total_credits_30d,
    -- Enrich recommendation with co-signal context
    case
        when cs.signal_count > 0
            then 'Actionable — ' || cs.fix_categories || ' optimization(s) available'
        else e.recommendation
    end as recommendation,
    case
        when cs.signal_count > 0
            then e.recommendation_reason || ' Root cause: ' || cs.fix_categories
                || ' signal(s) detected for this model.'
        else e.recommendation_reason
    end as recommendation_reason,
    case
        when cs.signal_count > 0 then 'actionable'
        else e.backlog_status
    end as backlog_status,
    cs.fix_categories as co_occurring_fixes,
    coalesce(cs.signal_count, 0) as co_signal_count,
    e.project_name,
    e.snapshot_date
from expensive as e
left join co_signals as cs on cs.node_id = e.node_id
order by e.estimated_annual_cost_usd desc nulls last
limit 10
