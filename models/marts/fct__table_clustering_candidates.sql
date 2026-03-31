{#--
  Unified cross-platform clustering candidates (Option C).

  Works on Snowflake, BigQuery, Redshift, and Databricks by routing through
  platform-agnostic intermediate models. Jinja handles:
    1. SQL syntax differences (md5, date functions)
    2. Platform-specific scoring (BQ uses bytes billed; others use execution time + partitions)

  For a BigQuery-native version with more granular BQ metrics, see:
    models/marts/bigquery/fct_bigquery__table_clustering_candidates.sql
--#}
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='clustering_candidates_snapshot_key'
  )
}}

{% set lookback_days = var('clustering_candidates_lookback_days', 7) %}
{% set min_size_gb = var('clustering_candidates_min_size_gb', 1000) %}
{% set dbt_project_only = var('clustering_candidates_dbt_project_only', true) %}
{% set target_databases = var('clustering_candidates_target_databases', []) %}
{% set target_schemas = var('clustering_candidates_target_schemas', []) %}

with large_tables as (
    select
        ti.database_name,
        ti.schema_name,
        ti.table_name,
        ti.active_bytes as size_bytes,
        ti.size_gb,
        ti.row_count,
        ti.is_already_clustered,
        ti.approx_micropartitions,
        ti.normalized_table_type as table_type
    from {{ ref('int_table_inventory') }} as ti
    where ti.size_gb >= {{ min_size_gb }}
        {% if target_databases and target_databases | length > 0 %}
            and upper(ti.database_name) in (
                {% for db in target_databases %}
                    '{{ db | upper }}'{% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        {% endif %}
        {% if target_schemas and target_schemas | length > 0 %}
            and upper(ti.schema_name) in (
                {% for sc in target_schemas %}
                    '{{ sc | upper }}'{% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        {% endif %}
    order by size_gb desc
    limit 100
),

table_query_stats as (
    select
        lt.database_name,
        lt.schema_name,
        lt.table_name,
        coalesce(sum(tqs.select_count), 0) as select_count,
        coalesce(sum(tqs.dml_count), 0) as dml_count,
        case
            when coalesce(sum(tqs.select_count), 0) > 0
            then coalesce(sum(tqs.select_execution_time_ms_sum), 0)
                 / nullif(sum(tqs.select_count), 0)
            else 0
        end as avg_execution_time_ms,
        case
            when coalesce(sum(tqs.select_count), 0) > 0
            then coalesce(sum(tqs.select_partitions_scanned_sum), 0)
                 / nullif(sum(tqs.select_count), 0)
            else 0
        end as avg_partitions_scanned,
        case
            when coalesce(sum(tqs.select_count), 0) > 0
            then coalesce(sum(tqs.select_partitions_total_sum), 0)
                 / nullif(sum(tqs.select_count), 0)
            else 0
        end as avg_partitions_total{% if target.type == 'bigquery' %},
        -- avg_bytes_billed: BigQuery-only cost signal flowing through the router's select *
        case
            when coalesce(sum(tqs.select_count), 0) > 0
            then coalesce(sum(tqs.select_bytes_billed_sum), 0)
                 / nullif(sum(tqs.select_count), 0)
            else 0
        end as avg_bytes_billed
        {% endif %}
    from large_tables as lt
    left join {{ ref('int_table_query_stats_daily') }} as tqs
        on upper(lt.database_name) = upper(tqs.table_database)
        and upper(lt.schema_name) = upper(tqs.table_schema)
        and upper(lt.table_name) = upper(tqs.table_name)
        and tqs.stats_date >= {{ dbt.dateadd('day', -lookback_days, 'current_date()') }}
    group by 1, 2, 3
),

scored as (
    select
        current_timestamp() as analyzed_at,
        lt.database_name,
        lt.schema_name,
        lt.table_name,
        upper(lt.database_name) || '.' || upper(lt.schema_name) || '.' || upper(lt.table_name) as table_fqn,
        dm.dbt_model,
        lt.table_type,
        coalesce(tqs.select_count, 0) as select_count,
        coalesce(tqs.dml_count, 0) as dml_count,
        coalesce(tqs.avg_execution_time_ms, 0) as avg_execution_time_ms,
        coalesce(tqs.avg_partitions_scanned, 0) as avg_partitions_scanned,
        coalesce(tqs.avg_partitions_total, 0) as avg_partitions_total,
        {% if target.type == 'bigquery' %}
        coalesce(tqs.avg_bytes_billed, 0) as avg_bytes_billed,
        {% endif %}
        lt.size_gb,
        coalesce(lt.row_count, 0) as row_count,
        -- micropartitions: falls back to approx_micropartitions when avg_partitions_total = 0
        -- For BigQuery: avg_partitions_total = 0, so always uses approx_micropartitions = total_partitions
        coalesce(
            nullif(coalesce(tqs.avg_partitions_total, 0), 0),
            lt.approx_micropartitions
        ) as micropartitions
    from large_tables as lt
    left join table_query_stats as tqs
        on upper(lt.database_name) = upper(tqs.database_name)
        and upper(lt.schema_name) = upper(tqs.schema_name)
        and upper(lt.table_name) = upper(tqs.table_name)
    left join {{ ref('int_dbt__relations') }} as dm
        on upper(lt.database_name) = upper(dm.database_name)
        and upper(lt.schema_name) = upper(dm.schema_name)
        and upper(lt.table_name) = upper(dm.table_name)
),

final as (
    select
        current_timestamp() as analyzed_at,
        current_date() as snapshot_date,
        -- Surrogate key: platform-specific md5 encoding
        {% if target.type == 'bigquery' %}
        to_hex(md5(cast(current_date() as string) || '|' || coalesce(table_fqn, '')))
        {% elif target.type == 'snowflake' %}
        md5(to_varchar(current_date()) || '|' || coalesce(table_fqn, ''))
        {% else %}
        md5(cast(current_date() as varchar) || '|' || coalesce(table_fqn, ''))
        {% endif %}
            as clustering_candidates_snapshot_key,
        database_name,
        schema_name,
        table_name,
        table_fqn,
        dbt_model,
        table_type,
        (
            case
                when select_count > 0 then
                    {% if target.type == 'bigquery' %}
                    -- BigQuery: score on avg GB billed per query (primary cost signal)
                    -- Higher bytes billed = more data scanned = more benefit from clustering
                    (select_count * (avg_bytes_billed / power(1024, 3)))
                    {% else %}
                    -- Snowflake/others: score on execution time (avg seconds per query × volume)
                    (select_count * (avg_execution_time_ms / 1000))
                    {% endif %}
                    -- read-heavy bonus: shared across all platforms
                    + ((select_count / case when dml_count = 0 then 1 else dml_count end) * 10)
                else 0
            end
        )
        * (
            -- partition density multiplier: shared across all platforms
            -- micropartitions = total_partitions for BigQuery (via approx_micropartitions fallback)
            case
                when row_count > 0 and (micropartitions / row_count) * 100 > 0.0001
                    then (micropartitions / row_count) * 100
                else 1
            end
        ) as score,
        case
            when
                select_count > 0
                and (select_count / case when dml_count = 0 then 1 else dml_count end) > 1
                and size_gb >= {{ min_size_gb }}
            then true
            else false
        end as is_candidate,
        size_gb as table_size_gb,
        row_count as total_rows,
        micropartitions as current_micropartitions,
        case
            when micropartitions > 0 then round(row_count / micropartitions, 2)
            else 0
        end as avg_rows_per_micropartition,
        avg_partitions_scanned,
        select_count,
        dml_count,
        round(select_count / (dml_count + 1), 1) as query_to_dml_ratio,
        round(avg_execution_time_ms / 1000, 2) as avg_query_duration_s
    from scored
    where
        {% if dbt_project_only %}
            dbt_model is not null
        {% else %}
            1 = 1
        {% endif %}
)

select
    analyzed_at,
    snapshot_date,
    clustering_candidates_snapshot_key,
    database_name,
    schema_name,
    table_name,
    table_fqn,
    dbt_model,
    table_type,
    score,
    is_candidate,
    table_size_gb,
    total_rows,
    current_micropartitions,
    avg_rows_per_micropartition,
    avg_partitions_scanned,
    select_count,
    dml_count,
    query_to_dml_ratio,
    avg_query_duration_s
from final
{% if is_incremental() %}
where snapshot_date >= (
    select coalesce(
        max(snapshot_date),
        cast('1970-01-01' as date)
    )
    from {{ this }}
)
{% endif %}
