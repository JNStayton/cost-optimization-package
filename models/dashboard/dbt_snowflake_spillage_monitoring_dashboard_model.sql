-- if you opt to have these monitoring queries in dbt:
    -- recommend creating a dedicated schema for analytics/dashboard models, e.g. "dbt_analytics"
    -- recommend scheduling this model to run weekly to keep the data fresh
    -- recommend placing these models either in your main dbt project or in a dedicated dbt project for monitoring/analytics (e.g. "dbt_monitoring")
    -- choose your desired materialization strategy below (recommend incremental)
    -- make sure the dbt user that is running the job for these models has access to the SNOWFLAKE database and ACCOUNT_USAGE schema to be able to query the views used here. you can create a specific dbt_monitoring_role for this purpose and grant it the necessary privileges.
    -- configure your Snowflake dashboard to point to this model to visualize the results and configure the desired lookback window (e.g. 7 days) there. this way users can view the results in the dashboard without needing access to the account usage views or run the queries directly against Snowflake.

{{ 
  config(
    materialized='incremental',
    unique_key=['query_date', 'database_name', 'schema_name', 'table_name', 'query_hash'],
    incremental_strategy='merge',
    schema='dbt_analytics' 
  ) 
}}

with dbt_sessions as (
    select distinct
        session_id
    from
        snowflake.account_usage.sessions
    where
        created_on >= dateadd(hour, -2, 
            {% if is_incremental() %}
                (select max(query_date) from {{ this }})
            {% endif %}
        )
        and parse_json(client_environment):APPLICATION::string = 'dbt'
),

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
        -- only pull new query data since the last model run
        q.start_time >= dateadd(hour, -2, 
            {% if is_incremental() %}
                (select max(query_date) from {{ this }})
            {% endif %}
        )
        and q.execution_status = 'SUCCESS'
)

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
    sum(round(w.bytes_spilled_to_local_storage / power(1024, 3), 2)) as total_gb_spilled_local,
    sum(round(w.bytes_spilled_to_remote_storage / power(1024, 3), 2)) as total_gb_spilled_remote,
    avg(round(w.bytes_spilled_to_local_storage / power(1024, 3), 2)) as avg_gb_spilled_local,
    avg(round(w.bytes_spilled_to_remote_storage / power(1024, 3), 2)) as avg_gb_spilled_remote,
    any_value(w.query_text) as sample_query_text

from
    dbt_writes w
group by
    1, 2, 3, 4, 5