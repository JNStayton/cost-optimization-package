{#-
  Phase F2: one view per chain. demo_chain_base_view → demo_chain_mid_view →
  demo_chain_step (ephemeral) → demo_chain_table.
    - Both views were probed (int_snowflake__view_probe, status ok), and their recompute
      cost comes from the probe.
    - Exactly one is 'recommended' and the other an 'alternative' naming it in
      chosen_view_for_chain and its reason. The recommended one has the higher net
      savings (seconds of recompute removed).
    - In the gold layer, only the recommended view is actionable; the alternative is
      monitor, so the cost-savings summary doesn't add the two together.
  Probe times are real, so the test checks the choice is consistent with them rather
  than naming a winner. Returns rows only on failure.
-#}
with chain as (
    select
        lower(model_name) as model_name, table_fqn, chain_role, chosen_view_for_chain,
        recompute_cost_source, net_recompute_s_saved, recommendation_reason
    from {{ ref('fct_snowflake__table_materialization_candidates') }}
    where lower(model_name) in ('demo_chain_base_view', 'demo_chain_mid_view')
),

probes as (
    select lower(split_part(view_fqn, '.', 3)) as model_name, probe_status, execution_time_ms
    from {{ ref('int_snowflake__view_probe') }}
    where lower(split_part(view_fqn, '.', 3)) in ('demo_chain_base_view', 'demo_chain_mid_view')
),

recommended as (select * from chain where chain_role = 'recommended'),
alternative as (select * from chain where chain_role = 'alternative'),

gold as (
    select lower(model_name) as model_name, backlog_status
    from {{ ref('int_snowflake__all_recommendations') }}
    where signal_id = 'materialize_as_table'
      and lower(model_name) in ('demo_chain_base_view', 'demo_chain_mid_view')
),

checks as (
    select 'two chain views in the fact' as check_name, (select count(*) from chain) = 2 as passed
    union all select 'ephemeral is never a candidate',
        not exists (select 1 from {{ ref('fct_snowflake__table_materialization_candidates') }}
                    where lower(model_name) = 'demo_chain_step')
    union all select 'both views probed ok',
        (select count(*) from probes where probe_status = 'ok' and execution_time_ms > 0) = 2
    union all select 'both costs from the probe',
        (select count(*) from chain where recompute_cost_source = 'probe') = 2
    union all select 'one recommended, one alternative',
        (select count(*) from recommended) = 1 and (select count(*) from alternative) = 1
    union all select 'alternative names the recommended view',
        (select a.chosen_view_for_chain = r.table_fqn
                and contains(a.recommendation_reason, 'Alternative to materializing ' || r.model_name)
         from alternative as a, recommended as r)
    union all select 'recommended has the higher net savings',
        (select r.net_recompute_s_saved >= a.net_recompute_s_saved from alternative as a, recommended as r)
    union all select 'gold: recommended actionable, alternative monitor',
        (select count(*) from gold g join recommended r on r.model_name = g.model_name
         where g.backlog_status = 'actionable') = 1
        and (select count(*) from gold g join alternative a on a.model_name = g.model_name
             where g.backlog_status = 'monitor') = 1
)

select * from checks where not coalesce(passed, false)
