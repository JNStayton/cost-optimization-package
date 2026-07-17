{#--
  Derives current warehouse configuration state from WAREHOUSE_EVENTS_HISTORY.
  One row per warehouse with its current size, type, Gen2 status, and multi-cluster state.

  This is the authoritative source for warehouse metadata — more reliable than
  warehouse_size from QUERY_HISTORY which can be null for many queries.
--#}
{{
  config(
    materialized='table',
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
    end as warehouse_category
from latest_consistent as lc
left join multicluster_evidence as mc
    on lc.warehouse_name = mc.warehouse_name
