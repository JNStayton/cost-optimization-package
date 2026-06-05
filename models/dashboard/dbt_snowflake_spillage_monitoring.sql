-- we are looking back 7 days in this query, you'll want to adjust that lookback window to suit your needs in the where clauses of both CTEs


-- the first CTE finds the session ids for all dbt sessions (where dbt authenticated and ran queries)
-- note: if you implement query tags, we can filter by those instead of client_environment for better accuracy and performance. query tags would also allow us to easily filter for specific dbt models/DAGs, which is a common use case for this type of monitoring.
with dbt_sessions as (
    select distinct
        session_id
    from
        snowflake.account_usage.sessions
    where
        created_on >= dateadd(day, -7, current_timestamp()) 
        and parse_json(client_environment):APPLICATION::string = 'dbt'
),

-- the second CTE uses that session id to pull query information, warehouse information, and table information for all queries that modified/built dbt-managed tables/views
dbt_writes as (
    select
        q.query_id,
        q.start_time,
        q.query_hash,
        q.warehouse_name,
        q.warehouse_size,
        q.total_elapsed_time,
        q.bytes_spilled_to_local_storage,
        q.bytes_spilled_to_remote_storage,
        q.query_text,
        t.table_catalog as database_name,
        t.table_schema as schema_name,
        t.table_name as table_name
    from
        snowflake.account_usage.query_history q
    inner join
        dbt_sessions s on q.session_id = s.session_id
    inner join
        snowflake.account_usage.access_history h on q.query_id = h.query_id
    , lateral flatten(input => h.objects_modified) f
    inner join
        snowflake.account_usage.tables t on f.value:objectId::number = t.table_id
    where
        q.start_time >= dateadd(day, -7, current_timestamp())
        and h.query_start_time >= dateadd(day, -7, current_timestamp())
        and q.execution_status = 'SUCCESS'
        -- optional: only look at tables or views (excludes stages, etc.)
        -- AND f.value:objectDomain::string IN ('Table', 'View')
)

-- the final select groups by the query_hash/query_date/fqn and provides sample query_id and query text, and aggregated metrics for spillage and query runtime (average per query and total of all queries)
select
    date_trunc('day', w.start_time) as query_date,
    w.database_name,
    w.schema_name,
    w.table_name,
    w.query_hash,

    -- some context
    any_value(w.query_id) as sample_query_id,
    any_value(w.warehouse_name) as warehouse_name,
    any_value(w.warehouse_size) as warehouse_size,
    count(w.query_id) as num_runs_per_day,

    -- runtime metrics
    sum(round(w.total_elapsed_time / 1000 / 60, 1)) as total_elapsed_mins,
    avg(round(w.total_elapsed_time / 1000, 2)) as avg_elapsed_seconds,
    
    -- spillage metrics
    sum(round(w.bytes_spilled_to_local_storage / 1024 / 1024 / 1024, 2)) as total_gb_spilled_local,
    sum(round(w.bytes_spilled_to_remote_storage / 1024 / 1024 / 1024, 2)) as total_gb_spilled_remote,
    avg(round(w.bytes_spilled_to_local_storage / 1024 / 1024 / 1024, 2)) as avg_gb_spilled_local,
    avg(round(w.bytes_spilled_to_remote_storage / 1024 / 1024 / 1024, 2)) as avg_gb_spilled_remote,

    -- sample compiled sql sent by dbt
    any_value(w.query_text) as sample_query_text

from
    dbt_writes w
-- optional filtering
-- where
--     w.database_name in ('db_1', 'db_2')
--     and w.schema_name in ('schema_1', 'schema_2')
group by
    1, 2, 3, 4, 5
order by
    query_date desc;