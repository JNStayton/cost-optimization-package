-- monitoring spillage specifically for dbt models
-- this uses the access_history view, which requires Enterprise Edition or higher
-- NB: an account_admin role is required to access this view
with dbt_tables as (
    select distinct
        t.table_id,
        t.table_catalog as database_name,
        t.table_schema as schema_name,
        t.table_name
    from
        snowflake.account_usage.tables as t
    where
        t.table_owner = 'dbt_svc_role' -- replace with your dbt service role
    and 
        -- optional, only include base tables (exclude views, external tables, etc.)
        t.table_type = 'BASE TABLE'
),

query_writes as (
    select
        h.query_id,
        t.table_id,
        t.database_name,
        t.schema_name,
        t.table_name
    from
        snowflake.account_usage.access_history as h,
        lateral flatten(input => h.objects_modified) as modified_objects
    inner join
        dbt_tables as t
        on modified_objects.value:objectid::number = t.table_id
    where
        h.query_start_time >= dateadd('day', -7, current_timestamp())
        -- optional, ensure we only look at modifications to tables
        -- and modified_objects.value:objectdomain::varchar = 'table'
)

select
    -- model/table identifiers and run dates
    date_trunc('day', q.start_time) as query_date,
    w.database_name,
    w.schema_name,
    w.table_name,
    q.query_hash,
    any_value(q.query_id) as sample_query_id,
    any_value(q.warehouse_name) as warehouse_name,
    any_value(q.warehouse_size) as warehouse_size,
    count(q.query_id) as num_runs_per_day,

    -- metrics
    sum(round(q.total_elapsed_time / 1000 / 60, 1)) as total_elapsed_mins,
    sum(round(q.bytes_spilled_to_local_storage / 1024 / 1024 / 1024, 2)) as total_gb_spilled_local,
    sum(round(q.bytes_spilled_to_remote_storage / 1024 / 1024 / 1024, 2)) as total_gb_spilled_remote,
    avg(round(q.bytes_spilled_to_local_storage / 1024 / 1024 / 1024, 2)) as avg_gb_spilled_local,
    avg(round(q.bytes_spilled_to_remote_storage / 1024 / 1024 / 1024, 2)) as avg_gb_spilled_remote,
    any_value(q.query_text) as sample_query_text

from
    snowflake.account_usage.query_history as q
inner join
    query_writes as w
    on q.query_id = w.query_id
where
    q.start_time >= dateadd('day', -7, current_timestamp())
and q.warehouse_size is not null
and w.database_name in ('<db_name>') -- optional: filter by specific databases
group by
    1, 2, 3, 4, 5
order by
    query_date desc;


