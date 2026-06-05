-- 1. Query Credit Consumption - Projected to Yearly Consumption
-- What it is: This chart shows a high-level trend of our daily credit usage over the last 7 days, aggregated on the query_hash level to avoid double counting repeated runs of the same query.

-- Purpose: This is our primary health check. We look at this to understand our baseline spend and quickly spot any anomalies. If we see a sudden, unexpected spike on a particular day, it's our first signal to start investigating.

-- How to read the output: 
-- query_text_preview: A snippet of the query text for context.
-- sample_query_id: A sample query ID for further investigation.
-- avg_daily_credits_for_query: The average daily credits consumed by this query over the last 7 days.
-- estimated_annual_credits: The projected annual credits based on the 7-day average.
-- estimated_annual_cost_usd: The projected annual cost in USD, assuming a rate of $2 per credit.
-- query_hash: The unique identifier for the query structure, useful for grouping similar queries.

with query_costs_in_period as (
    select
        qh.query_hash,
        any_value(qh.query_id) as sample_query_id,
        substring(min(qh.query_text), 1, 150) as query_text_preview,
        count(distinct qh.query_id) as total_runs_last_7_days,
        sum(qah.credits_attributed_compute) as total_credits_last_7_days
    from
        snowflake.account_usage.query_history as qh
    join
        snowflake.account_usage.query_attribution_history as qah
        on qh.query_id = qah.query_id
    where
        qh.database_name in ('db_1', 'db_2') -- optionally filter by databases
        and qh.start_time >= dateadd(day, -7, current_timestamp()) -- last 7 days
        and qh.query_text ilike '%"app":"dbt"%' -- optionally filter only dbt models. if you use query tags, you can filter by those instead for better performance and accuracy.
        and qah.credits_attributed_compute > 0 -- only queries that have meaningful costs
    group by
        qh.query_hash
)
-- calculate the estimated annual cost from the 7-day average
select
    query_text_preview,
    sample_query_id,
    total_runs_last_7_days,
    total_credits_last_7_days,
    (total_credits_last_7_days / 7) as avg_daily_credits_for_query,
    (avg_daily_credits_for_query * 365) as estimated_annual_credits,
    (estimated_annual_credits * 2) as estimated_annual_cost_usd,
    query_hash
from
    query_costs_in_period
where
    total_credits_last_7_days > 0.1 -- optional: filter out insignificant queries
order by
    estimated_annual_cost_usd desc
limit 100; 