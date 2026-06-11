{#--
  Derives current warehouse configuration state from WAREHOUSE_EVENTS_HISTORY.
  One row per warehouse with its current size, type, Gen2 status, and multi-cluster state.
--#}
{{
  config(
    materialized='table',
    enabled=(target.type == 'snowflake')
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
    where event_name = 'WAREHOUSE_CONSISTENT'
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
    coalesce(lc.warehouse_type, '') = 'SNOWPARK-OPTIMIZED' as is_snowpark_optimized
from latest_consistent as lc
left join multicluster_evidence as mc
    on lc.warehouse_name = mc.warehouse_name
