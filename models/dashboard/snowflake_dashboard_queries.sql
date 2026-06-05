-- These queries are for your Snowflake Cost Monitoring dashboard Together, they helps us move from being reactive to proactive by answering three key questions:

-- What are we spending our money on?
-- Why is it costing that much?
-- How can we make it more efficient?

-- 1. Daily Credit Consumption
-- What it is: This chart shows a high-level trend of our daily credit usage over the last two weeks.
-- Purpose: This is our primary health check. We look at this to understand our baseline spend and quickly spot any anomalies. If we see a sudden, unexpected spike on a particular day, it's our first signal to start investigating.
-- Noteworthy metrics: Total credits consumed calculated how many credits have been used per diem, and this value is then used to give us an estimated annual cost (were we to continue this query pattern on the same cadence without any changes) 

SELECT
    qh.query_id,
    DATE_TRUNC('day', qh.start_time)::date AS query_date,
    SUM(qah.credits_attributed_compute) AS total_credits_consumed,
    SUM(qah.credits_attributed_compute) * 2 * 365 as total_estimated_annual_cost -- replace 2 with your actual credit per dollar rate as per your account contract
FROM
    snowflake.account_usage.query_history AS qh
JOIN
    snowflake.account_usage.query_attribution_history AS qah
    ON qh.query_id = qah.query_id
WHERE
    qh.database_name in ('DB_1', 'DB_2') -- optionally filter by databases rather than entire account
    AND qh.start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()) -- last 7 days
    AND qah.credits_attributed_compute > 0
GROUP BY
    qh.query_id,
    query_date
ORDER BY
    qh.query_id,
    query_date;


with total_cost_in_period as (
    select
        sum(qah.credits_attributed_compute) as total_credits_last_7_days
    from
        snowflake.account_usage.query_history as qh
    join
        snowflake.account_usage.query_attribution_history as qah
        on qh.query_id = qah.query_id
    where
        qh.database_name in ('DB_1', 'DB_2') -- optionally filter by databases rather than entire account
        and qh.start_time >= dateadd(day, -7, current_timestamp()) -- last 7 days
        and qah.credits_attributed_compute > 0
)
select
    total_credits_last_7_days,
    (total_credits_last_7_days / 7) as avg_daily_credits,
    (avg_daily_credits * 365) as estimated_annual_credits,
    (estimated_annual_credits * 2) as estimated_total_annual_cost_usd -- replace 2 with your actual credit per dollar rate as per your account contract
FROM
    total_cost_in_period;


-- 2. Top Expensive Queries
-- What it is: This is a ranked list of the individual queries that have consumed the most credits recently.
-- Purpose: This is our optimization hit list. If we want to make the biggest impact on our bill, we start here. We can see who ran the query, which warehouse they used, and a preview of the SQL itself. This tile tells us exactly where to focus our refactoring efforts.

SELECT
    qh.query_id,
    qah.credits_attributed_compute as total_credits,
    round(qah.credits_attributed_compute * 2 * 365, 2) as estimated_annual_cost, -- replace 2 with your actual credit per dollar rate as per your account contract
    (qh.total_elapsed_time / 1000) * 100 AS duration_seconds,
    qh.user_name,
    qh.warehouse_name,
    SUBSTRING(qh.query_text, 1, 2000) AS query_text_preview,
FROM
    snowflake.account_usage.query_history AS qh
JOIN
    snowflake.account_usage.query_attribution_history AS qah
    ON qh.query_id = qah.query_id
WHERE
    qh.database_name in ['DB_1', 'DB_2'] -- optionally filter by databases rather than entire account
    AND qh.start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()) -- last 7 days
    AND qah.credits_attributed_compute > 0
    AND qh.query_text not like '%CREATE TABLE%' -- optionally filter out non-analytical queries
ORDER BY
    qah.credits_attributed_compute DESC
LIMIT 20;


-- 3. Queries with Spilled Data
-- What it is: This tile isolates queries that are "spilling to disk," which means they're running out of memory.
-- Purpose: This is a technical diagnostic tool. Spillage is a huge red flag for inefficiency and a clear sign that a query is too complex for the given warehouse size. This list tells us exactly which queries would be targets for SQL refactoring and optimization or a larger warehouse.

SELECT
    query_id,
    warehouse_name,
    warehouse_size,
    round(total_elapsed_time / 1000 / 60, 1) AS elapsed_mins,
    round(bytes_spilled_to_local_storage / 1024 / 1024 / 1024, 2) AS gb_spilled_to_local,
    round(bytes_spilled_to_remote_storage / 1024 / 1024 / 1024, 2) AS gb_spilled_to_remote
FROM
    snowflake.account_usage.query_history
WHERE
    start_time >= dateadd('day', -7, current_timestamp()) -- last 7 days
    AND warehouse_size IS NOT NULL
    AND (bytes_spilled_to_local_storage > 0 OR bytes_spilled_to_remote_storage > 0)
ORDER BY
    gb_spilled_to_local DESC,
    gb_spilled_to_remote DESC
LIMIT 10;


-- 4. Top Expensive Warehouses
-- What it is: A simple breakdown of which virtual warehouses are consuming the most credits.
-- Purpose: This tile helps us understand cost allocation. Since we assign different warehouses to different teams or workloads, this shows us which business units or processes—like our production dbt jobs versus our BI tools—are the biggest drivers of our spend.

SELECT
    qh.warehouse_name,
    SUM(qah.credits_attributed_compute) AS total_credits_consumed,
    SUM(qah.credits_attributed_compute) * 2 as total_estimated_annual_cost -- replace 2 with your actual credit per dollar rate as per your account contract
FROM
    snowflake.account_usage.query_history AS qh
JOIN
    snowflake.account_usage.query_attribution_history AS qah
    ON qh.query_id = qah.query_id
WHERE
    qh.database_name in ['DB_1', 'DB_2'] -- optionally filter by databases rather than entire account
    AND qh.start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()) -- last 7 days
    AND qah.credits_attributed_compute > 0
GROUP BY
    qh.warehouse_name
ORDER BY
    total_credits_consumed DESC
LIMIT 10;


-- 5. Warehouse Usage Metrics 
-- What it is: This shows us the average load for each warehouse.
-- Purpose: This is our efficiency report card. There are a lot of metrics here – An ideal warehouse should have a healthy average load (~70-80%). If a warehouse consistently has a very low load, it's likely over-provisioned, and we can save money by scaling it down. If it is consistently high (85+%), you might want to consider sizing up. This helps us "right-size" our compute. 

select 
    warehouse_name,
    warehouse_size,
    round(avg(case when query_load_percent > 0 then query_load_percent else null end)) as avg_cluster_load,
    sum(case when query_load_percent = 100 then 1 else 0 end) as queries_100_pct_load,
    count(*) as count_queries
from   
    snowflake.account_usage.query_history
where  
    warehouse_size is not null
and    
    execution_status = 'SUCCESS' -- errors don't reflect actual load and are evaluated separately
and    
    start_time < dateadd('days',-7,current_timestamp()) -- last 7 days
group by all
order by 
    warehouse_size, 
    warehouse_name;


-- 6. Top Expensive Errors
-- What it is: This tile lists queries that failed but still consumed a significant amount of credits before they crashed.
-- Purpose: This is our "pure waste" report. Every credit on this list was spent on a query that produced no value. This helps us identify and fix queries that are silently costing us money without anyone realizing it.

SELECT
    qah.credits_attributed_compute as credits_wasted,
    qah.credits_attributed_compute * 2 as estimated_cost_usd, -- replace 2 with your actual credit per dollar rate as per your account contract
    (qh.total_elapsed_time / 1000) AS duration_seconds,
    qh.user_name,
    qh.warehouse_name,
    qh.database_name,
    qh.error_message,
    SUBSTRING(qh.query_text, 1, 200) AS query_text_preview,
    qh.query_id
FROM
    snowflake.account_usage.query_history AS qh
JOIN
    snowflake.account_usage.query_attribution_history AS qah
    ON qh.query_id = qah.query_id
WHERE
    qh.start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()) -- last 7 days
    AND qh.error_code IS NOT NULL
    -- Filter to find queries that consumed a non-trivial amount of credits before failing
    AND qah.credits_attributed_compute > 0.01
ORDER BY
    qah.credits_attributed_compute DESC
LIMIT 20;


-- 7. Frequently Occurring Errors
-- What it is: A list of the most common error messages happening across the platform.
-- Purpose: This helps us spot systemic issues. If we see the same permission error appearing hundreds of times, it tells us we have a configuration problem to solve. Fixing the root cause of these frequent errors can save a lot of developer time and frustration.

SELECT
    error_message,
    COUNT(*) AS error_count
FROM
    snowflake.account_usage.query_history
WHERE
    start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()) -- last 7 days
    AND error_code IS NOT NULL -- Filter for only queries that produced an error
GROUP BY
    error_message
ORDER BY
    error_count DESC
LIMIT 10; -- increase limit if you want to see more errors


-- 8. Credits by Error Type
-- What it is: This chart aggregates the total credits wasted for each type of error.
-- Purpose: This helps us prioritize which errors to fix first. While an error might happen frequently, it might not be very costly. This view combines frequency with financial impact, showing us which error types are having the biggest negative effect on our budget.

SELECT
    qh.error_code,
    qh.error_message as sample_error_message,
    -- Sum the credits consumed by queries before they failed
    COALESCE(SUM(qah.credits_attributed_compute), 0) AS total_credits_wasted
FROM
    snowflake.account_usage.query_history AS qh
LEFT JOIN
    snowflake.account_usage.query_attribution_history AS qah
    ON qh.query_id = qah.query_id
WHERE
    qh.start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()) -- last 7 days
    AND qh.error_code IS NOT NULL
GROUP BY
    qh.error_code,
    qh.error_message
HAVING
    total_credits_wasted > 0 -- Only show errors that wasted a measurable amount of credits
ORDER BY
    total_credits_wasted DESC
LIMIT 10; -- increase limit if you want to see more errors


-- 9. Top Expensive Users
-- What it is: A leaderboard showing which users or service accounts have consumed the most credits.
-- Purpose: This tile helps us understand who is driving our usage. It's not about blaming individuals, but about identifying power users who might benefit from training on cost optimization, or service accounts that might be running inefficiently scheduled jobs.

SELECT
    qh.user_name,
    SUM(qah.credits_attributed_compute) AS total_credits_consumed
FROM
    snowflake.account_usage.query_history AS qh
JOIN
    snowflake.account_usage.query_attribution_history AS qah
    ON qh.query_id = qah.query_id
WHERE
    qh.database_name in ['DB_1', 'DB_2'] -- optionally filter by databases rather than entire account
    AND qh.start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()) -- last 7 days
    AND qah.credits_attributed_compute > 0
GROUP BY
    qh.user_name
ORDER BY
    total_credits_consumed DESC
LIMIT 5;