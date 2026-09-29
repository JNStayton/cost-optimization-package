{#-
  End to end: int_snowflake__all_recommendations, the backlog every gold view reads, holds
  exactly the recommendations the earlier slices produce, with the right status.
    - min_annual_savings_usd ($1 default): DEMO_EVENTS clustering saves ~$0.24/yr, so it's
      demoted to 'stable'; the incremental and materialization recommendations ($5–8/yr)
      stay 'actionable'.
    - Scope: warehouse-level recommendations appear only for warehouses this project's
      models run on. FIXTURE_WH_BUSY qualifies; IDLE, COLD, BUSY_XS, BUSY_2XL, BUSY_6XL and
      SUSPENDED run no project models, and OVERSIZED runs another project's.
    - DEMO_SESSIONS (key probe failed) is 'monitor'; the expensive demo_logs query is
      'monitor'. DEMO_FAST_GROWTH and DEMO_NEW_TABLE have no recommendation.
  Entities are compared by their last name part (table or warehouse name, or query hash).
  Returns rows only on mismatch.
-#}
with produced as (
    select domain, signal_id, lower(split_part(entity_name, '.', -1)) as entity, backlog_status
    from {{ ref('int_snowflake__all_recommendations') }}
),

expected as (
    select 'clustering' as domain, 'add_clustering_key_evaluate' as signal_id, 'demo_events' as entity, 'stable' as backlog_status
    union all select 'materialization', 'apply_incremental_merge',    'demo_orders',             'actionable'
    union all select 'materialization', 'apply_incremental_merge',    'demo_infrequent_builds',  'actionable'
    union all select 'materialization', 'apply_incremental_append',   'demo_logs',               'actionable'
    union all select 'materialization', 'apply_incremental_merge',    'demo_sessions',           'monitor'
    union all select 'materialization', 'materialize_as_table',       'demo_slow_view',          'actionable'
    union all select 'warehouse',       'overload_scale_up_standard', 'fixture_wh_busy',         'actionable'
    union all select 'warehouse',       'expensive_query_actionable', 'hash_fixture_wh_busy',    'actionable'
    union all select 'warehouse',       'expensive_query_monitor',    'hash_fixture_wh_healthy', 'monitor'
)

select
    coalesce(p.domain, e.domain) as domain,
    coalesce(p.signal_id, e.signal_id) as signal_id,
    coalesce(p.entity, e.entity) as entity,
    p.backlog_status as produced_status,
    e.backlog_status as expected_status
from produced as p
full outer join expected as e
    on p.domain = e.domain and p.signal_id = e.signal_id and p.entity = e.entity
where p.backlog_status is distinct from e.backlog_status
