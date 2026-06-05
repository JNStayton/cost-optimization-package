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
        and qh.query_text ilike '%"app":"dbt"%' -- optionally filter only dbt models
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



-- Monitoring spillage over time
select
    query_id,
    start_time,
    warehouse_name,
    warehouse_size,
    round(total_elapsed_time / 1000 / 60, 1) AS elapsed_mins,
    round(bytes_spilled_to_local_storage / 1024 / 1024 / 1024, 2) AS gb_spilled_to_local,
    round(bytes_spilled_to_remote_storage / 1024 / 1024 / 1024, 2) AS gb_spilled_to_remote,
    query_text 
from
    snowflake.account_usage.query_history
where
    start_time >= dateadd('day', -7, current_timestamp())
    and warehouse_size is not null
    -- track only runs with spillage, but since we also want to see improvements I have this commented out
    -- and (bytes_spilled_to_local_storage > 0 or bytes_spilled_to_remote_storage > 0)
    -- below are the types of filters you might want to apply
    -- specifically for dbt models
    and query_text ilike '%"app":"dbt"%' 
    -- specifically for a given model
    and query_text ilike '%<table_name>%'
    -- specifically for the initial or full build of that model
    and query_text ilike '%CREATE%<table_name>%'
    -- For incremental runs, you might want to track INSERT or MERGE statements as well
    and query_text ilike '%INSERT%<table_name>%'
    and query_text ilike '%MERGE%<table_name>%'

order by
    start_time desc
-- optionally limit results
limit 100;