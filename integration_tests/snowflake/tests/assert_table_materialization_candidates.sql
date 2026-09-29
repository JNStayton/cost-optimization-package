{#-
  fct_snowflake__table_materialization_candidates, fed by the fixture query history,
  recommends materializing the slow, frequently queried demo view, monitors the quiet
  one, and omits the rarely queried one. Returns rows only on mismatch.
-#}
with produced as (
    select lower(model_name) as model_name, recommendation, select_count
    from {{ ref('fct_snowflake__table_materialization_candidates') }}
    where startswith(lower(model_name), 'demo_')
),

expected as (
    select 'demo_slow_view' as model_name, 'Materialize as TABLE' as recommendation, 60 as select_count
    union all select 'demo_quiet_view', 'Monitor', 15
)

select
    coalesce(p.model_name, e.model_name) as model_name,
    p.recommendation as produced_recommendation, e.recommendation as expected_recommendation,
    p.select_count   as produced_select_count,   e.select_count   as expected_select_count
from produced as p
full outer join expected as e on p.model_name = e.model_name
where p.recommendation is distinct from e.recommendation
   or p.select_count   is distinct from e.select_count
