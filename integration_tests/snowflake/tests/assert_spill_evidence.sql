{#-
  Phase S2b, Enterprise edition: measured spillage effects.
    - int_snowflake__query_spill_evidence: the real spilling query (sampled for
      demo_spill_heavy_large) has spilling operators blocked on disk, and its
      spill_blocked_s = execution time x their disk I/O share; the non-spilling control
      (sampled for demo_spill_heavy_small) has no spilling operators and 0 s blocked,
      though its table scan spends most of its time on disk I/O.
      The fake query IDs are recorded as skipped.
    - SQL refactor (demo_spill_heavy_large, 2X-Large, 32 credits/hour): savings = the
      sample's blocked time scaled to all its spilling queries (x 660 / 60 s) x 32 / 3600
      x 365/30 x $2, to the cent from the measured share; hours saved = that time / 3600
      x 365/30.
    - Scale-ups (savings null): hours saved = T x (1 - 1 / (2 x eff)) / 3600 x 365/30 and
      cost change = T x rate x (1/eff - 1) / 3600 x 365/30 x $2. demo_spill_heavy_small
      on BUSY (Medium, eff 0.86, 4/hour, T 250 s): 0.35 h, +$1.10. demo_spill_remote on
      IDLE (Small, eff 1.00, T 120 s): 0.20 h, $0. The IDLE aggregate (T 276 s): 0.47 h, $0.
  Standard edition: no table-level spillage, so no evidence and no rows. Returns rows
  only on failure.
-#}
{%- if var('snowflake_enterprise_edition', true) %}
with evidence as (
    select lower(split_part(table_fqn, '.', 3)) as table_name, evidence_status, execution_time_s,
           spilling_operator_count, blocked_on_disk_share, spill_blocked_s
    from {{ ref('int_snowflake__query_spill_evidence') }}
),

measured as (
    select execution_time_s, spill_blocked_s
    from evidence
    where table_name = 'demo_spill_heavy_large' and evidence_status = 'ok'
),

recs as (
    select signal_id, lower(split_part(entity_name, '.', -1)) as entity,
           estimated_annual_savings_usd, estimated_annual_hours_saved, estimated_annual_cost_change_usd
    from {{ ref('int_snowflake__all_recommendations') }}
    where signal_id in ('spillage_sql_refactor', 'spillage_scale_up')
),

checks as (
    select 'spilling query: spilling operators blocked on disk' as check_name,
        (select count(*) from evidence
         where table_name = 'demo_spill_heavy_large' and evidence_status = 'ok'
           and spilling_operator_count > 0 and spill_blocked_s > 0
           and abs(spill_blocked_s - execution_time_s * blocked_on_disk_share) < 0.001) = 1 as passed
    union all select 'control: no spilling operators, 0 s blocked',
        (select count(*) from evidence
         where table_name = 'demo_spill_heavy_small' and evidence_status = 'ok'
           and spilling_operator_count = 0 and spill_blocked_s = 0) = 1
    union all select 'fake query IDs are skipped, not failed',
        (select count(*) from evidence where evidence_status = 'skipped' and spill_blocked_s is null) >= 2
    union all select 'SQL refactor savings from the measured blocked time',
        (select round(r.estimated_annual_savings_usd, 2)
                = round(m.spill_blocked_s * 660 / m.execution_time_s * 32 / 3600.0 * 365 / 30 * 2, 2)
         from recs as r, measured as m
         where r.signal_id = 'spillage_sql_refactor' and r.entity = 'demo_spill_heavy_large')
    union all select 'SQL refactor hours saved',
        (select r.estimated_annual_hours_saved
                = round(m.spill_blocked_s * 660 / m.execution_time_s / 3600.0 * 365 / 30, 2)
         from recs as r, measured as m
         where r.signal_id = 'spillage_sql_refactor' and r.entity = 'demo_spill_heavy_large')
    union all select 'scale-ups: savings null, hours saved and cost change',
        (select listagg(entity || ':' || coalesce(estimated_annual_savings_usd::varchar, 'null') || ':'
                        || estimated_annual_hours_saved::number(10, 2) || ':'
                        || estimated_annual_cost_change_usd::number(10, 2), ',')
                within group (order by entity)
         from recs where signal_id = 'spillage_scale_up')
        = 'demo_spill_heavy_small:null:0.35:1.10,demo_spill_remote:null:0.20:0.00,fixture_wh_idle:null:0.47:0.00'
)

select * from checks where not coalesce(passed, false)
{%- else %}
select 1 as unexpected_row
from {{ ref('int_snowflake__query_spill_evidence') }}
{%- endif %}
