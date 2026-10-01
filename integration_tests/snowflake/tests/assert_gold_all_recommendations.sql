{#-
  End to end: int_snowflake__all_recommendations, the backlog every gold view reads, holds
  exactly the recommendations the earlier slices produce, with the right status.
    - min_annual_savings_usd ($1 default): DEMO_EVENTS clustering saves ~$0.50/yr, so it's
      demoted to 'stable'; the incremental and materialization recommendations ($13–75/yr)
      stay 'actionable'.
    - Scope: warehouse-level recommendations appear only for warehouses this project's
      models run on. FIXTURE_WH_BUSY runs the expensive demo_orders query; IDLE, COLD and
      BUSY_2XL run the spillage slice's dbt builds. BUSY_XS, BUSY_6XL and SUSPENDED run no
      project models, and OVERSIZED runs another project's.
    - Warehouse savings: IDLE 30 auto-suspend cycles x (300 - 60) s / 3600 x 2/hour
      (Small) x 12 x $2 = $96; COLD and BUSY_2XL 6 credits x 10% x 12 x $2 = $14.40.
    - DEMO_SESSIONS (key probe failed) is 'monitor'; the expensive demo_logs query is
      'monitor'. DEMO_FAST_GROWTH and DEMO_NEW_TABLE have no recommendation.
    - Savings (to the cent) check the pricing: each model at its own build warehouse's list
      rate (e.g. demo_orders: 400 s x 0.23 builds/day x 365 x 4/hour (Medium) / 3600 x $2
      x 0.9952 redundancy = $74.26), X-Small (1/hour) when unknown, and materialization and
      clustering counts annualized by 365 / their lookback window (14 and 7 days).
      demo_slow_view: (60 reads + 10 rollup builds - 4 view builds) x 45 s / 3600 x 365/14
      x $2 = $43.02. DEMO_EVENTS: its filter share is 10 of 10 analyzed queries, not 10 of
      20 reads.
    - View chain: demo_chain_base_view and demo_chain_mid_view are alternatives; the one
      recommended is actionable, the other monitor (assert_view_chain_selection checks
      which). Their savings come from real probe times, so only their status is compared
      here, by role.
  Entities are compared by their last name part (table or warehouse name, or query hash).
  Returns rows only on mismatch.
-#}
{#- FIXTURE_WH_BUSY's config signal depends on the edition (multi-cluster is Enterprise). -#}
{%- set busy_signal = 'overload_enable_mcw' if var('snowflake_enterprise_edition', true) else 'overload_scale_up_standard' %}
{%- set busy_2xl_signal = busy_signal %}
with produced as (
    select ar.domain, ar.signal_id,
           -- Chain views by role (the probe decides which view wins)
           coalesce('chain_' || tm.chain_role, lower(split_part(ar.entity_name, '.', -1))) as entity,
           ar.backlog_status,
           iff(tm.chain_role is not null, null, round(ar.estimated_annual_savings_usd, 2)) as savings
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    left join {{ ref('fct_snowflake__table_materialization_candidates') }} as tm
        on tm.table_fqn = ar.table_fqn
       and ar.signal_id = 'materialize_as_table'
       and startswith(lower(tm.model_name), 'demo_chain_')
),

expected as (
    select 'clustering' as domain, 'add_clustering_key_evaluate' as signal_id, 'demo_events' as entity, 'stable' as backlog_status, 0.50 as savings
    union all select 'materialization', 'apply_incremental_merge',    'demo_orders',             'actionable',  74.26
    union all select 'materialization', 'apply_incremental_merge',    'demo_infrequent_builds',  'actionable',  13.72
    union all select 'materialization', 'apply_incremental_append',   'demo_logs',               'actionable',  37.13
    union all select 'materialization', 'apply_incremental_merge',    'demo_sessions',           'monitor',     18.57
    union all select 'materialization', 'materialize_as_table',       'demo_slow_view',          'actionable',  43.02
    union all select 'materialization', 'materialize_as_table',       'chain_recommended',       'actionable',  null
    union all select 'materialization', 'materialize_as_table',       'chain_alternative',       'monitor',     null
    union all select 'warehouse',       '{{ busy_signal }}', 'fixture_wh_busy',         'actionable', 144.00
    union all select 'warehouse',       'expensive_query_actionable', 'hash_fixture_wh_busy',    'actionable', 277.40
    union all select 'warehouse',       'expensive_query_monitor',    'hash_fixture_wh_healthy', 'monitor',     27.74
    union all select 'warehouse',       'idle_reduce_auto_suspend',   'fixture_wh_idle',         'actionable',  96.00
    union all select 'warehouse',       'provisioning_gen2',          'fixture_wh_cold',         'actionable',  14.40
    union all select 'warehouse',       '{{ busy_2xl_signal }}',      'fixture_wh_busy_2xl',     'actionable',  14.40
{%- if var('snowflake_enterprise_edition', true) %}
    {#- Spillage (Enterprise edition): signal and status from the tier key. Scale-ups'
        GB-based savings are pennies, so the $1 floor demotes them to stable. #}
    union all select 'warehouse', 'spillage_sql_refactor',       'demo_spill_heavy_large', 'actionable', 4.48
    union all select 'warehouse', 'spillage_scale_up',           'demo_spill_heavy_small', 'stable',     0.28
    union all select 'warehouse', 'spillage_scale_up',           'demo_spill_remote',      'stable',     0.32
    union all select 'warehouse', 'spillage_scale_up',           'fixture_wh_idle',        'stable',     0.49
    union all select 'warehouse', 'spillage_moderate_worsening', 'demo_spill_worsening',   'monitor',    0.23
    union all select 'warehouse', 'spillage_moderate_stable',    'demo_spill_steady',      'monitor',    0.02
    union all select 'warehouse', 'spillage_moderate_stable',    'demo_spill_minor',       'stable',     0.00
{%- endif %}
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
