{# Passthrough of sys_query_detail. Materialization decisions live in
   int_redshift__query_detail.

   For a 'scan' step, input_bytes is always 0 (a scan is a leaf node in the
   execution plan, so it has no upstream step to receive input from) —
   output_bytes is the column that reflects what the scan actually read from
   storage, and is what any consumer measuring bytes scanned should sum. #}

with source as (

    select * from {{ source('redshift_usage', 'query_detail') }}

),

renamed as (

    select
        query_id,
        child_query_sequence,
        segment_id,
        step_id,
        trim(step_name)     as step_name,
        table_id,
        trim(table_name)    as table_name,
        trim(source)        as scan_source,
        input_rows,
        output_rows,
        input_bytes,
        output_bytes,
        blocks_read,
        duration,
        trim(metrics_level) as metrics_level,
        start_time

    from source

)

select * from renamed
