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