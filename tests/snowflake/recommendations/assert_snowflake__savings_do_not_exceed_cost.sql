{#--
  A recommendation can't save more than the cost it addresses. Every domain's savings
  estimate is its cost estimate times a factor of at most 1 (rebuild redundancy,
  0.7 for spillage, 0.2 for expensive queries, the clustering filter share and scan
  reduction), or the cost minus the work that remains after the change
  (materialization). A row here means a cost or savings formula is wrong.

  Runs on real data in any project that builds the package on Snowflake. Warns rather
  than errors, so a package bug doesn't fail the project's build. Returns the offending
  recommendations.
--#}
select
    domain,
    signal_id,
    entity_name,
    estimated_annual_cost_usd,
    estimated_annual_savings_usd
from {{ ref('int_snowflake__all_recommendations') }}
where estimated_annual_savings_usd is not null
  and estimated_annual_cost_usd is not null
  -- a cent of tolerance for floating-point rounding
  and estimated_annual_savings_usd > estimated_annual_cost_usd + 0.01
