-- Top Expensive Warehouses (dbt-Scoped)
-- what it is: a breakdown of which virtual warehouses are consuming the most credits.
-- this tile helps us understand cost allocation. since we assign different warehouses to different teams or workloads, this shows us which business units or processes are the biggest drivers of our spend.

with dbt_sessions as (
    select distinct
        session_id
    from
        snowflake.account_usage.sessions
    where
        created_on >= dateadd(day, -7, current_timestamp()) 
        and parse_json(client_environment):APPLICATION::string = 'dbt'
),

dbt_queries as (
    select
        qh.query_id,
        qh.warehouse_name
    from
        snowflake.account_usage.query_history qh
    inner join
        dbt_sessions s on qh.session_id = s.session_id
    where
        qh.start_time >= dateadd(day, -7, current_timestamp()) -- last 7 days
        and qh.execution_status = 'SUCCESS'
)

select
    dq.warehouse_name,
    sum(qah.credits_attributed_compute) as total_credits_consumed,
    sum(qah.credits_attributed_compute) * 2 as total_cost -- replace 2 with actual rate
from
    dbt_queries dq
join
    snowflake.account_usage.query_attribution_history as qah
    on dq.query_id = qah.query_id
where
    qah.credits_attributed_compute > 0
group by
    dq.warehouse_name
order by
    total_credits_consumed desc
limit 10;



-- Warehouse Usage Metrics 
-- What it is: This shows us the average load for each warehouse.
-- there are a lot of metrics here... an ideal warehouse should have a healthy average load (~70-80%). if a warehouse consistently has a very low load, it's likely over-provisioned, and we can save money by scaling it down. If it is consistently high (85+%), you might want to consider sizing up. This helps us right-size our compute. 

with dbt_sessions as (
    select distinct
        session_id
    from
        snowflake.account_usage.sessions
    where
        created_on >= dateadd(day, -7, current_timestamp()) 
        and parse_json(client_environment):APPLICATION::string = 'dbt'
),

dbt_query_stats as (
    select 
        qh.warehouse_name,
        qh.warehouse_size,
        qh.query_load_percent,
        qh.total_elapsed_time,
        qh.session_id
    from   
        snowflake.account_usage.query_history qh
    inner join
        dbt_sessions s on qh.session_id = s.session_id
    where  
        qh.warehouse_size is not null
        and qh.execution_status = 'SUCCESS'
        and qh.start_time >= dateadd('days', -7, current_timestamp()) -- last 7 days
)

select 
    warehouse_name,
    warehouse_size,
    round(avg(case when query_load_percent > 0 then query_load_percent else null end)) as avg_cluster_load,
    sum(case when query_load_percent = 100 then 1 else 0 end) as queries_100_pct_load,
    count(*) as count_dbt_queries
from   
    dbt_query_stats
group by all
order by 
    warehouse_size, 
    warehouse_name;


-- idle credit consumption by day and warehouse
-- if we see idle credit consumption, we can evaluate our auto-suspend settings and consider implementing or adjusting them to reduce idle spend. We can also look at the query patterns and see if there are specific times of day or days of the week when idle spend is higher, which can inform scheduling decisions for workloads that run on those warehouses.
select
    date_trunc('day', start_time) as usage_day,
    warehouse_name,
    sum(credits_used) as total_credits,
    sum(credits_used_compute) as execution_credits,
    (sum(credits_used) - sum(credits_used_compute)) as idle_credits
from
    snowflake.account_usage.warehouse_metering_history
where
    start_time >= dateadd(day, -7, current_timestamp())
    -- optionally filter by specific warehouses
    -- and warehouse_name in ('wh_1', 'wh_2')
group by
    1, 2
-- optionally only include days with idle credit spend
-- having
--     idle_credits > 0
order by
    warehouse_name, usage_day desc;


-- daily average provisioning queue time by warehouse
-- side by side with the above query this should give a good overview of whether our solution to the idle credit consumption is affecting our query performance (i.e. if we adjust our auto-suspend to avoid idle credit consumption, are we seeing an increase in provisioning queue time for queries hitting those warehouses?)
select
    date_trunc('day', start_time) as query_date,
    warehouse_name,
    sum(queued_provisioning_time) / 1000 as daily_total_provisioning_seconds,
    avg(queued_provisioning_time) / 1000 as daily_avg_provisioning_seconds_per_query,
    count(query_id) as total_queries
from
    snowflake.account_usage.query_history
where
    start_time >= dateadd(day, -7, current_timestamp())
    -- optionally filter by specific warehouses
    -- and warehouse_name in ('wh_1', 'wh_2')
group by
    1, 2
order by
    warehouse_name, query_date desc;


-- peak clusters spun up per hour for a specific warehouse (last 7 days)
-- this helps us understand how our multi-cluster warehouses are scaling to meet demand over time
select
    date_trunc('hour', start_time) as time_period,
    max(cluster_number) as max_clusters_spun_up
from
    snowflake.account_usage.query_history
where
    warehouse_name in ('wh_1', 'wh_2') -- specify your warehouses here
    and start_time >= dateadd(day, -7, current_timestamp()) -- last 7 days
    and cluster_number is not null -- filter out non-compute queries (e.g., metadata)
group by
    time_period
order by
    time_period desc;

-- we could flesh this out further with metrics like warehouse credit consumption by joining to the warehouse_metering_history view
-- we could also filter by query_text to focus on specific workloads, like dbt models (e.g., and query_text ilike '%"app":"dbt"%') to understand how our dbt transformations are scaling with warehouse clusters.
-- you could further pull in specific query_ids to analyze performance during peak cluster usage.
-- alternatively, if query tags are implemented, we could filter by those for better accuracy and performance instead of parsing query_text.

----------------------------------------------------------------------

-- This query summarizes the total queue time for each warehouse, splitting it by the two main reasons: warehouse provisioning or overload.
-- by provisioning, we mean the time that a query spent waiting for the warehouse to resume from a suspended state or to be resized.
-- by overload, we mean the time that a query spent waiting because the warehouse was already at capacity handling other queries.
-- overload is the easier fix. we can either decrease the threads in dbt or increase the warehouse clusters (for multicluster warehouses) to address concurrency issues.
-- provisioning delays can be addressed by ensuring the warehouse is kept running during known busy periods or by scheduling dbt runs when the warehouse is already active.
select
    warehouse_name,
    date_trunc('day', start_time)::date as query_date,
    sum(queued_provisioning_time) / 1000 as total_provisioning_queue_seconds,
    sum(queued_overload_time) / 1000 as total_overload_queue_seconds
from
    snowflake.account_usage.query_history
where
    start_time >= dateadd(day, -30, current_timestamp())
    and (queued_provisioning_time > 0 or queued_overload_time > 0)
group by
    warehouse_name,
    query_date
order by
    total_overload_queue_seconds desc,
    total_provisioning_queue_seconds desc;

----------------------------------------------------------------------

-- This query identifies the specific recurring queries (by query_hash) that are contributing the most to your total queue time.
-- if we see warehouses with high queue times above, we can use this to pinpoint which queries are the main culprits and evaluate running them with a separate, dedicated warehouse.

select
    qh.query_hash,
    substring(min(qh.query_text), 1, 150) as query_text_preview,
    qh.warehouse_name,
    any_value(qh.user_name) as sample_user,
    count(*) as total_queued_runs,
    sum(qh.queued_provisioning_time) / 1000 as total_provisioning_queue_seconds,
    sum(qh.queued_overload_time) / 1000 as total_overload_queue_seconds,
    (total_provisioning_queue_seconds + total_overload_queue_seconds) as total_queue_time_seconds
from
    snowflake.account_usage.query_history qh
where
    qh.start_time >= dateadd(day, -7, current_timestamp())
    and (qh.queued_provisioning_time > 0 or qh.queued_overload_time > 0)
    -- optionally filter only dbt models
    and qh.query_text ilike '%"app":"dbt"%'
    -- optionally filter by specific warehouses
    and qh.warehouse_name in ('wh_1', 'wh_2')
group by
    qh.query_hash, 
    qh.warehouse_name
order by
    total_overload_queue_seconds desc,
    total_provisioning_queue_seconds desc
-- optionally limit results
limit 50;

-- if total_provisioning_queue_seconds is high, your warehouse is likely suspending and resuming too often. your auto_suspend time may be too low for your workload patterns. however, we do want to balance this with idle warehouse credit consumption. keep in mind that SOME queueing is healthy, especially for batch load processes.
-- if total_overload_queue_seconds is high, your warehouse is likely undersized for your workload (a single complex query may be bottle-necking your jobs) or you have too many concurrent queries. consider increasing the warehouse size, pulling out complex queries to run on a separate warehouse, or enabling multi-cluster scaling if not already in place.

----------------------------------------------------------------------
-- other tips:

-- workload segregation: use separate warehouses for different workloads to prevent resource contention.
-- auto-suspend settings: adjust auto-suspend times based on workload patterns to balance cost, idle time, and performance.
-- proactively resume warehouses: schedule warehouses to resume before known busy periods to avoid provisioning delays. you can do this with a Snowflake task that runs on a schedule to "wake up" your warehouse before a big job run. this is really only useful if you have very large warehouses that take time to resume.