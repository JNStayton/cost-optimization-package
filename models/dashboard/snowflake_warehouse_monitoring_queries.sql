-- monitor idle credit consumption by day and warehouse

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
    

-- monitor daily average provisioning queue time by warehouse; side by side with the above query this should give a good overview of whether our solution to the idle credit consumption is affecting our query performance
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