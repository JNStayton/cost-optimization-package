-- Purpose: Identifies dbt workloads bottlenecked by concurrency (QUEUED_OVERLOAD_TIME) to recommend optimal scaling (MCW or single-cluster scale up/down).
-- This helps ensure that warehouses are right-sized for the workload, improving performance and cost-efficiency.
-- For checking whole Snowflake account, remove the dbt_sessions CTE and related join.


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
        qh.total_elapsed_time,
        qh.queued_overload_time,
        qh.query_type -- Added to track DML activity
    from
        snowflake.account_usage.query_history qh
    inner join
        dbt_sessions s on qh.session_id = s.session_id
    where
        qh.start_time >= dateadd(day, -7, current_timestamp())
        and qh.execution_status = 'SUCCESS'
        and qh.warehouse_size is not null
),

warehouse_summary as (
    select
        warehouse_size,
        warehouse_name,
        count(*) as total_dbt_queries,
        -- Calculate the percentage of queries that are DML; this is specific to dbt workloads and can help inform whether Gen2 warehouses are recommended
        count(case when query_type in ('MERGE', 'UPDATE', 'DELETE', 'INSERT') then 1 end) * 1.0 / count(*) as dml_ratio,
        median(total_elapsed_time / 1000) as median_execution_sec,
        median(queued_overload_time / 1000) as median_overload_sec
    from
        dbt_query_stats
    group by 1, 2
    having count(*) >= 20 
)

select
    warehouse_name,
    warehouse_size,
    total_dbt_queries,
    round(dml_ratio * 100, 1) as dml_percentage, -- helpful for visibility
    round(median_execution_sec, 2) as median_execution_sec,
    round(median_overload_sec, 2) as median_overload_sec,
    
    case
        -- threshold: if more than 35% of queries are DML, recommend Gen2 warehouses, which are optimized for DML workloads. You can adjust this threshold based on your specific workload characteristics and tolerance for DML contention.
        -- depending on the Snowflake edition you are on, you may have access to Gen2 warehouses, which are highly recommended for workloads that are DML-heavy. If you don't want to include Gen2 Warehouses in your recommendation logic, you can remove that condition.
        when dml_ratio > 0.35 
        then 'recommend enabling gen2 (dml heavy workload)'

        when median_overload_sec > 5 and median_overload_sec > (median_execution_sec * 0.1)
        then 'enterprise and enterprise plus editions only: recommend multi-cluster (high concurrency bottlenecks identified); otherwise consider splitting your workload into multiple warehouses'
        
        when median_overload_sec > 1 and median_overload_sec <= 5
        then 'scale up single cluster (moderate concurrency bottlenecks identified)'
        
        when median_execution_sec < 5 and median_overload_sec < 0.5
        then 'scale down single cluster (oversized for the given workload complexity)'
        
        else 'stable (optimize sql or evaluate scheduling if performance is not as expected)'
    end as configuration_recommendation
from
    warehouse_summary
order by
    dml_ratio desc;