{# Passthrough of sys_query_history. Materialization decisions live in
   int_redshift__query_history. #}

with source as (
    select * from {{ source('redshift_usage', 'query_history') }}
)

, renamed as (
    select
        -- id
        query_id,

        -- foreign keys
        user_id,
        transaction_id,
        session_id,
        service_class_id,
        result_cache_query_id,

        -- integers
        query_hash_version,
        returned_rows as rows_produced, -- rename to align to Pat's Snowflake naming convention

        -- floats
        -- sys_query_history's time columns are documented by AWS as microseconds,
        -- not milliseconds: https://docs.aws.amazon.com/redshift/latest/dg/SYS_QUERY_HISTORY.html
        -- Divide by 10^6, not 10^3, to actually land in seconds.
        cast(elapsed_time / 1000000.0 as float) as elapsed_time_seconds, -- cast to float as elapsed query time can vary substantially so specific precision can't be guaranteed
        cast(queue_time / 1000000.0 as float) as queue_time_seconds,
        cast(execution_time / 1000000.0 as float) as execution_time_seconds,
        cast(compile_time / 1000000.0 as float) as compile_time_seconds,
        cast(planning_time / 1000000.0 as float) as planning_time_seconds,
        cast(lock_wait_time / 1000000.0 as float) as lock_wait_time_seconds,
        cast(returned_bytes / 10^9 as float) as gigabytes_returned, -- convert to GB (not GiB)
        
        -- varchars
        username as user_name,
        query_label,
        database_name,
        query_type,
        status as execution_status,
        error_message,
        query_text,
        redshift_version,
        usage_limit,
        compute_type,
        service_class_name,
        query_priority,
        short_query_accelerated,
        user_query_hash,
        generic_query_hash,

        -- booleans
        result_cache_hit as is_result_cache_hit,
        
        -- timestamps
        start_time,
        end_time
        

    from source
)

select * from renamed