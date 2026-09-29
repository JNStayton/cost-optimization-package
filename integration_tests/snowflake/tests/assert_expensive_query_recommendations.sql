{#-
  fct_snowflake__expensive_query_recommendations lists this project's recurring dbt
  queries by projected cost. Credits are estimated from elapsed time (the attribution
  fixture is empty), and each fixture query hash has its warehouse's hour to itself:
    - demo_orders on FIXTURE_WH_BUSY: 24 runs, 6 days x 9.5 compute credits = 57 credits,
      57 / 30 x 365 x $2 = $1,387/yr → above the $500 threshold → review
    - demo_logs on FIXTURE_WH_HEALTHY: 24 runs, 5.7 credits, $138.70/yr → monitor
    - model.other_project.big_model is another project's → excluded by scope_filter
    - queries with no dbt node id → excluded
  Returns rows only on mismatch.
-#}
with produced as (
    select dbt_node_id, total_runs_30d, total_credits_30d, estimated_annual_cost_usd, recommendation,
           credits_from_attribution
    from {{ ref('fct_snowflake__expensive_query_recommendations') }}
    where startswith(warehouse_name, 'FIXTURE_WH_')
),

expected as (
    select 'model.cost_optimization_integration_tests.demo_orders' as dbt_node_id, 24 as total_runs_30d,
           57.0 as total_credits_30d, 1387.00 as estimated_annual_cost_usd,
           'Review for refactor opportunities (high projected cost)' as recommendation, false as credits_from_attribution
    union all select 'model.cost_optimization_integration_tests.demo_logs', 24, 5.7, 138.70,
           'Monitor — recurring credit consumption', false
)

select
    coalesce(p.dbt_node_id, e.dbt_node_id) as dbt_node_id,
    p.total_runs_30d            as produced_runs,    e.total_runs_30d            as expected_runs,
    p.total_credits_30d         as produced_credits, e.total_credits_30d         as expected_credits,
    p.estimated_annual_cost_usd as produced_cost,    e.estimated_annual_cost_usd as expected_cost,
    p.recommendation            as produced_rec,     e.recommendation            as expected_rec
from produced as p
full outer join expected as e on p.dbt_node_id = e.dbt_node_id
where p.total_runs_30d            is distinct from e.total_runs_30d
   or p.total_credits_30d         is distinct from e.total_credits_30d
   or p.estimated_annual_cost_usd is distinct from e.estimated_annual_cost_usd
   or p.recommendation            is distinct from e.recommendation
   or p.credits_from_attribution  is distinct from e.credits_from_attribution
