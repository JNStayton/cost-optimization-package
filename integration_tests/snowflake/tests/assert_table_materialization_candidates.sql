{#-
  fct_snowflake__table_materialization_candidates, fed by the fixture query history,
  recommends materializing the slow, frequently queried demo view, monitors the quiet
  one, and omits the rarely queried one. demo_slow_view feeds demo_slow_view_rollup: 10 of
  its dbt builds count (the same-named non-dbt table elsewhere doesn't), and dbt created
  the view 4 times. The quiet view feeds nothing and has no builds (minimum 1).
  View chain slice: both chain views get demo_chain_table's 10 builds (every table
  downstream counts, not only the one each feeds directly), and dbt created each twice.
  The ephemeral demo_chain_step is never a candidate.
  Returns rows only on mismatch.
-#}
with produced as (
    select lower(model_name) as model_name, recommendation, select_count, downstream_build_count, view_build_runs
    from {{ ref('fct_snowflake__table_materialization_candidates') }}
    where startswith(lower(model_name), 'demo_')
),

expected as (
    select 'demo_slow_view' as model_name, 'Materialize as TABLE' as recommendation, 60 as select_count,
           10 as downstream_build_count, 4 as view_build_runs
    union all select 'demo_quiet_view', 'Monitor', 15, 0, 1
    union all select 'demo_chain_base_view', 'Materialize as TABLE', 200, 10, 2
    union all select 'demo_chain_mid_view',  'Materialize as TABLE', 150, 10, 2
)

select
    coalesce(p.model_name, e.model_name) as model_name,
    p.recommendation as produced_recommendation, e.recommendation as expected_recommendation,
    p.select_count   as produced_select_count,   e.select_count   as expected_select_count,
    p.downstream_build_count as produced_downstream_builds, e.downstream_build_count as expected_downstream_builds,
    p.view_build_runs        as produced_view_builds,       e.view_build_runs        as expected_view_builds
from produced as p
full outer join expected as e on p.model_name = e.model_name
where p.recommendation is distinct from e.recommendation
   or p.select_count   is distinct from e.select_count
   or p.downstream_build_count is distinct from e.downstream_build_count
   or p.view_build_runs        is distinct from e.view_build_runs
