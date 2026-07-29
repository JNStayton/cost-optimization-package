{#--
  Derives current warehouse configuration state from WAREHOUSE_EVENTS_HISTORY
  and enriches with live settings from SHOW WAREHOUSES (via post-hook macro).

  One row per warehouse with: current size, type, Gen2 status, multi-cluster state,
  auto_suspend, auto_resume, scaling_policy, min/max cluster counts.

  The base SQL derives what it can from events. The post-hook (refresh_warehouse_config)
  runs SHOW WAREHOUSES and merges auto_suspend/resume/scaling settings that aren't
  available in any ACCOUNT_USAGE view.
--#}
{{
  config(
    materialized='table',
    post_hook="{{ refresh_warehouse_config() }}"
  )
}}

with latest_consistent as (
    select
        warehouse_name,
        warehouse_size,
        cluster_count,
        warehouse_type,
        resource_constraint,
        event_timestamp
    from {{ ref('stg_snowflake__warehouse_events_history') }}
    qualify row_number() over (partition by warehouse_name order by event_timestamp desc) = 1
),

multicluster_evidence as (
    select
        warehouse_name,
        max(cluster_number) as max_observed_clusters
    from {{ ref('stg_snowflake__warehouse_events_history') }}
    where event_reason = 'MULTICLUSTER_SPINUP'
        and event_timestamp >= dateadd('day', -90, current_timestamp())
    group by warehouse_name
)

select
    lc.warehouse_name,
    lc.warehouse_size as current_size,
    coalesce(lc.cluster_count, 1) as current_cluster_count,
    lc.warehouse_type as current_warehouse_type,
    lc.resource_constraint,
    lc.event_timestamp as config_observed_at,
    -- Derived flags
    coalesce(lc.resource_constraint, '') = 'STANDARD_GEN_2' as is_gen2,
    coalesce(mc.max_observed_clusters, coalesce(lc.cluster_count, 1)) > 1 as is_multicluster,
    coalesce(lc.warehouse_type, '') = 'ADAPTIVE' as is_adaptive,
    coalesce(lc.warehouse_type, '') = 'SNOWPARK-OPTIMIZED' as is_snowpark_optimized,
    -- Sizing guard: true if warehouse is already at smallest possible size
    lower(coalesce(lc.warehouse_size, '')) in ('x-small', 'xsmall') as is_smallest_size,
    -- Category for grouping/reporting
    case
        when coalesce(lc.warehouse_type, '') = 'ADAPTIVE' then 'adaptive'
        when coalesce(mc.max_observed_clusters, coalesce(lc.cluster_count, 1)) > 1 then 'multi_cluster'
        when coalesce(lc.resource_constraint, '') = 'STANDARD_GEN_2' then 'gen2'
        else 'standard'
    end as warehouse_category,
    -- SHOW WAREHOUSES columns (populated by post-hook, null until then)
    cast(null as int) as auto_suspend_seconds,
    cast(null as boolean) as auto_resume,
    cast(null as varchar) as scaling_policy,
    cast(null as int) as min_cluster_count,
    cast(null as int) as max_cluster_count
from latest_consistent as lc
left join multicluster_evidence as mc
    on lc.warehouse_name = mc.warehouse_name
