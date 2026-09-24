{{ config(materialized='table') }}

with table_info as (

    select
        database_name,
        schema_name,
        table_name,
        size_mb,
        percent_unsorted,
        total_rows,
        estimated_visible_rows

    from {{ ref('int_redshift__table_info') }}

),

scored as (

    select
        database_name,
        schema_name,
        table_name,
        size_mb,
        percent_unsorted,
        round(coalesce(percent_unsorted, 0) / 100.0, 4)     as unsorted_factor,
        total_rows,
        estimated_visible_rows,
        round(
            case
                when coalesce(total_rows, 0) = 0 then 0.0
                else 1.0 - (coalesce(estimated_visible_rows, 0)::float / total_rows::float)
            end,
            4
        )                                                    as bloat_factor

    from table_info

)

select
    database_name,
    schema_name,
    table_name,
    size_mb,
    percent_unsorted,
    unsorted_factor,
    total_rows,
    estimated_visible_rows,
    bloat_factor,
    round(
        (unsorted_factor + bloat_factor) * coalesce(size_mb, 0),
        2
    )                                                        as priority_score

from scored
