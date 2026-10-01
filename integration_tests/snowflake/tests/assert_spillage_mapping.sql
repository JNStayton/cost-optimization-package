{#-
  Spillage recommendations in int_snowflake__all_recommendations take their signal and
  effort from the performance model's tier key (recommendation_key), not its text:
    - remote_spill, local_heavy_small_wh  → spillage_scale_up,           config_change
    - local_heavy_large_wh                → spillage_sql_refactor,       sql_refactor
    - local_moderate_worsening            → spillage_moderate_worsening, investigation
    - local_moderate_stable               → spillage_moderate_stable,    investigation
    - local_minor                         → spillage_moderate_stable,    config_change
  Text matching mislabeled three of these (heavy spill on a large warehouse and moderate
  stable spill as scale-ups; scale-ups as SQL refactors). Spillage needs Enterprise
  edition, so on Standard there are no spillage rows. Status is checked in
  assert_gold_all_recommendations. Returns rows only on mismatch.
-#}
with produced as (
    select lower(split_part(table_fqn, '.', -1)) as table_name, signal_id, effort_category
    from {{ ref('int_snowflake__all_recommendations') }}
    where signal_id like 'spillage%' and table_fqn is not null
),

expected as (
{%- if var('snowflake_enterprise_edition', true) %}
    select 'demo_spill_remote' as table_name, 'spillage_scale_up' as signal_id, 'config_change' as effort_category
    union all select 'demo_spill_heavy_small', 'spillage_scale_up',           'config_change'
    union all select 'demo_spill_heavy_large', 'spillage_sql_refactor',       'sql_refactor'
    union all select 'demo_spill_worsening',   'spillage_moderate_worsening', 'investigation'
    union all select 'demo_spill_steady',      'spillage_moderate_stable',    'investigation'
    union all select 'demo_spill_minor',       'spillage_moderate_stable',    'config_change'
    union all select 'demo_chain_table',       'spillage_moderate_stable',    'investigation'
{%- else %}
    select null::varchar as table_name, null::varchar as signal_id, null::varchar as effort_category
    where false
{%- endif %}
)

select
    coalesce(p.table_name, e.table_name) as table_name,
    p.signal_id       as produced_signal, e.signal_id       as expected_signal,
    p.effort_category as produced_effort, e.effort_category as expected_effort
from produced as p
full outer join expected as e on p.table_name = e.table_name
where p.signal_id is distinct from e.signal_id
   or p.effort_category is distinct from e.effort_category
