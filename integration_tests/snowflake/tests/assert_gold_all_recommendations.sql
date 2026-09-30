{#-
  End to end: int_snowflake__all_recommendations, the backlog every gold view reads, holds
  exactly the recommendations the earlier slices produce, with the right status.
    - min_annual_savings_usd ($1 default): DEMO_EVENTS clustering saves ~$0.50/yr, so it's
      demoted to 'stable'; the incremental and materialization recommendations ($13–75/yr)
      stay 'actionable'.
    - Scope: warehouse-level recommendations appear only for warehouses this project's
      models run on. FIXTURE_WH_BUSY qualifies; IDLE, COLD, BUSY_XS, BUSY_2XL, BUSY_6XL and
      SUSPENDED run no project models, and OVERSIZED runs another project's.
    - DEMO_SESSIONS (key probe failed) is 'monitor'; the expensive demo_logs query is
      'monitor'. DEMO_FAST_GROWTH and DEMO_NEW_TABLE have no recommendation.
    - Savings (to the cent) check the pricing: each model at its own build warehouse's list
      rate (e.g. demo_orders: 400 s x 0.23 builds/day x 365 x 4/hour (Medium) / 3600 x $2
      x 0.9952 redundancy = $74.26), X-Small (1/hour) when unknown, and materialization and
      clustering counts annualized by 365 / their lookback window (14 and 7 days).
  Entities are compared by their last name part (table or warehouse name, or query hash).
  Returns rows only on mismatch.
-#}
with produced as (
    select domain, signal_id, lower(split_part(entity_name, '.', -1)) as entity, backlog_status,
           round(estimated_annual_savings_usd, 2) as savings
    from {{ ref('int_snowflake__all_recommendations') }}
),

expected as (
    select 'clustering' as domain, 'add_clustering_key_evaluate' as signal_id, 'demo_events' as entity, 'stable' as backlog_status, 0.50 as savings
    union all select 'materialization', 'apply_incremental_merge',    'demo_orders',             'actionable',  74.26
    union all select 'materialization', 'apply_incremental_merge',    'demo_infrequent_builds',  'actionable',  13.72
    union all select 'materialization', 'apply_incremental_append',   'demo_logs',               'actionable',  37.13
    union all select 'materialization', 'apply_incremental_merge',    'demo_sessions',           'monitor',     18.57
    union all select 'materialization', 'materialize_as_table',       'demo_slow_view',          'actionable',  38.46
    union all select 'warehouse',       'overload_scale_up_standard', 'fixture_wh_busy',         'actionable', 144.00
    union all select 'warehouse',       'expensive_query_actionable', 'hash_fixture_wh_busy',    'actionable', 277.40
    union all select 'warehouse',       'expensive_query_monitor',    'hash_fixture_wh_healthy', 'monitor',     27.74
)

select
    coalesce(p.domain, e.domain) as domain,
    coalesce(p.signal_id, e.signal_id) as signal_id,
    coalesce(p.entity, e.entity) as entity,
    p.backlog_status as produced_status,
    e.backlog_status as expected_status,
    p.savings        as produced_savings,
    e.savings        as expected_savings
from produced as p
full outer join expected as e
    on p.domain = e.domain and p.signal_id = e.signal_id and p.entity = e.entity
where p.backlog_status is distinct from e.backlog_status
   or p.savings        is distinct from e.savings
