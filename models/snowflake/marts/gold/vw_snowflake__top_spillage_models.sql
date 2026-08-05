{{
  config(
    materialized='view',
  )
}}

{#--
  Top spillage models — models whose BUILD queries cause the most spillage.
  Useful for identifying where clustering or warehouse scaling has the biggest impact.

  Includes the most recent dbt Cloud run_id that caused spillage for traceability.

  Audience: dbt developers, Snowflake admins.
  Grain: one row per model (table_fqn).
  Ordered by total spillage descending.
--#}

with spillage_models as (
    select
        sp.table_fqn,
        sp.model_name,
        sp.dbt_model as node_id,
        sp.warehouse_name,
        sp.total_gb_spilled_local,
        sp.total_gb_spilled_remote,
        sp.total_gb_spilled_local + sp.total_gb_spilled_remote as total_gb_spilled,
        sp.spill_trend,
        sp.spill_days,
        sp.recommendation,
        sp.recommendation_reason,
        sp.snowflake_ddl,
        sp.snapshot_date,
        case
            when split_part(sp.dbt_model, '.', 2) = '{{ project_name }}'
                then 'project'
            else 'installed_package'
        end as model_source
    from {{ ref('fct_snowflake__warehouse_performance_recommendations') }} as sp
    where sp.recommendation not like 'Not available%'
      and sp.dbt_model is not null
),

-- Find the most recent spilling build query per model for run_id traceability
recent_spilling_builds as (
    select
        parse_json(regexp_substr(query_text, '/\\*\\s*(\\{.+\\})\\s*\\*/', 1, 1, 'e')):node_id::string as node_id,
        dbt_cloud_run_id,
        dbt_cloud_job_id,
        query_start_time,
        round(bytes_spilled_local / power(1024, 3), 3) as gb_spilled_local,
        row_number() over (
            partition by parse_json(regexp_substr(query_text, '/\\*\\s*(\\{.+\\})\\s*\\*/', 1, 1, 'e')):node_id::string
            order by query_start_time desc
        ) as rn
    from {{ ref('int_snowflake__query_history') }}
    where bytes_spilled_local > 0
      and query_text like '%node_id%'
      and query_start_time >= dateadd(day, -30, current_timestamp())
),

-- Priority context from int_all_recommendations
priority_context as (
    select
        table_fqn,
        min(priority_tier) as priority_tier,
        signal_id
    from {{ ref('int_snowflake__all_recommendations') }}
    where signal_id like 'spillage%'
    group by table_fqn, signal_id
)

select
    sm.model_name,
    sm.table_fqn,
    sm.node_id,
    sm.warehouse_name,
    sm.model_source,
    sm.total_gb_spilled_local,
    sm.total_gb_spilled_remote,
    sm.total_gb_spilled,
    sm.spill_trend,
    sm.spill_days,
    sm.recommendation,
    sm.recommendation_reason,
    sm.snowflake_ddl,
    pc.priority_tier,
    pc.signal_id,
    -- Most recent spilling run context
    rsb.dbt_cloud_run_id as last_spilling_run_id,
    rsb.dbt_cloud_job_id as last_spilling_job_id,
    rsb.query_start_time as last_spill_at,
    rsb.gb_spilled_local as last_spill_gb,
    sm.snapshot_date
from spillage_models as sm
left join recent_spilling_builds as rsb
    on rsb.node_id = sm.node_id
    and rsb.rn = 1
left join priority_context as pc
    on pc.table_fqn = sm.table_fqn
order by sm.total_gb_spilled desc
