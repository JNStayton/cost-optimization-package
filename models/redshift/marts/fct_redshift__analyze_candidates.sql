{{ config(materialized='table') }}

with table_info as (

    select
        database_name,
        schema_name,
        table_name,
        size_mb,
        statistics_staleness

    from {{ ref('int_redshift__table_info') }}

),

scored as (

    select
        database_name,
        schema_name,
        table_name,
        size_mb,
        statistics_staleness,
        round(coalesce(statistics_staleness, 0) / 100.0, 4)  as stats_off_factor

    from table_info

)

select
    database_name,
    schema_name,
    table_name,
    size_mb,
    statistics_staleness,
    stats_off_factor,
    round(
        stats_off_factor * coalesce(size_mb, 0),
        2
    )                                                        as priority_score

from scored
