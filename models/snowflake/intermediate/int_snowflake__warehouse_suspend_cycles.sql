{{
  config(
    materialized='table',
  )
}}

{#--
  Suspend and MCW spindown cycle counts per warehouse over the last 30 days.
  Used by int_snowflake__all_recommendations to compute data-driven idle
  credit savings instead of hardcoded assumptions.

  Grain: one row per warehouse_name.

  Event semantics:
    - SUSPEND_WAREHOUSE / WAREHOUSE_AUTOSUSPEND = full warehouse suspend cycle
      (auto_suspend timeout expired). Each cycle wastes up to auto_suspend_seconds
      of idle credits before the warehouse shuts down.
    - SUSPEND_CLUSTER / MULTICLUSTER_SPINDOWN = MCW extra cluster spindown.
      Under ECONOMY scaling, clusters idle ~150s before spindown.
      Under STANDARD scaling, spindown is near-immediate.
    - SPINUP_CLUSTER or RESUME_CLUSTER / MULTICLUSTER_SPINUP = MCW scale-out.
      Tracked for context (spinup-to-spindown ratio).
--#}

select
    warehouse_name,
    count(case
        when event_name = 'SUSPEND_WAREHOUSE'
         and event_reason = 'WAREHOUSE_AUTOSUSPEND'
        then 1
    end) as autosuspend_cycles_30d,
    count(case
        when event_name = 'SUSPEND_CLUSTER'
         and event_reason = 'MULTICLUSTER_SPINDOWN'
        then 1
    end) as mcw_spindown_cycles_30d,
    count(case
        when event_name in ('SPINUP_CLUSTER', 'RESUME_CLUSTER')
         and event_reason = 'MULTICLUSTER_SPINUP'
        then 1
    end) as mcw_spinup_cycles_30d
from {{ ref('stg_snowflake__warehouse_events_history') }}
where event_timestamp >= dateadd(day, -30, current_timestamp())
group by warehouse_name
