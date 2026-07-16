with source as (
    select * from {{ source('redshift_usage', 'table_info') }}
)

, renamed as (
    select
        -- ids
        table_id,

        -- integers
        max_varchar,
        sortkey_num,
        size as size_mb, -- size is in 1-MB data blocks
        tbl_rows as total_rows,
        estimated_visible_rows,

        -- floats / numerics
        pct_used as percent_used,
        unsorted as percent_unsorted,
        stats_off as statistics_staleness,
        skew_sortkey1,
        skew_rows,
        vacuum_sort_benefit,

        -- varchars
        trim(database) as database_name,
        trim(schema) as schema_name,
        trim("table") as table_name, -- "table" is a reserved word
        trim(encoded) as encoded,
        trim(diststyle) as distribution_style,
        trim(sortkey1) as sortkey1,
        trim(sortkey1_enc) as sortkey1_encoding,
        trim(risk_event) as risk_event,

        -- timestamps
        create_time

    from source
)

select * from renamed
