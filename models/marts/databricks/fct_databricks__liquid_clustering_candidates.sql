{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='liquid_clustering_candidates_snapshot_key'
  )
}}

{% set lookback_days = var('liquid_clustering_candidates_lookback_days', 7) %}
{% set min_size_gb = var('liquid_clustering_candidates_min_size_gb', 1) %}
{% set dbt_project_only = var('liquid_clustering_candidates_dbt_project_only', true) %}
{% set target_databases = var('liquid_clustering_candidates_target_databases', []) %}
{% set target_schemas = var('liquid_clustering_candidates_target_schemas', []) %}
{% set max_avg_file_size_mb = var('liquid_clustering_candidates_max_avg_file_size_mb', 128) %}

with large_tables as (
    select
        ti.database_name,
        ti.schema_name,
        ti.table_name,
        ti.active_bytes as size_bytes,
        ti.size_gb,
        ti.row_count,
        ti.is_already_clustered,
        ti.file_count,
        ti.normalized_table_type as table_type,
        case
            when ti.file_count > 0
            then (ti.active_bytes / 1024.0 / 1024.0) / ti.file_count
            else null
        end as avg_file_size_mb
    from {{ ref('int_table_inventory') }} as ti
    where ti.size_gb >= {{ min_size_gb }}
        and ti.file_count is not null
        {% if target_databases and target_databases | length > 0 %}
            and ti.database_name in (
                {% for db in target_databases %}
                    '{{ db }}'{% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        {% endif %}
        {% if target_schemas and target_schemas | length > 0 %}
            and ti.schema_name in (
                {% for sc in target_schemas %}
                    '{{ sc }}'{% if not loop.last %}, {% endif %}
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
            then coalesce(sum(tqs.select_execution_time_ms_sum), 0) / sum(tqs.select_count)
            else 0
        end as avg_execution_time_ms,
        case
            when coalesce(sum(tqs.select_count), 0) > 0
            then coalesce(sum(tqs.select_partitions_scanned_sum), 0) / sum(tqs.select_count)
            else 0
        end as avg_files_scanned
    from large_tables as lt
    left join {{ ref('int_table_query_stats_daily') }} as tqs
        on lt.database_name = tqs.table_database
        and lt.schema_name = tqs.table_schema
        and lt.table_name = tqs.table_name
        and tqs.stats_date >= current_date() - INTERVAL {{ lookback_days }} DAYS
    group by 1, 2, 3
),

scored as (
    select
        current_timestamp() as analyzed_at,
        lt.database_name,
        lt.schema_name,
        lt.table_name,
        lt.database_name || '.' || lt.schema_name || '.' || lt.table_name as table_fqn,
        dm.dbt_model,
        lt.table_type,
        coalesce(tqs.select_count, 0) as select_count,
        coalesce(tqs.dml_count, 0) as dml_count,
        coalesce(tqs.avg_execution_time_ms, 0) as avg_execution_time_ms,
        coalesce(tqs.avg_files_scanned, 0) as avg_files_scanned,
        lt.size_gb,
        coalesce(lt.row_count, 0) as row_count,
        lt.file_count,
        lt.avg_file_size_mb,
        lt.is_already_clustered
    from large_tables as lt
    left join table_query_stats as tqs
        on lt.database_name = tqs.database_name
        and lt.schema_name = tqs.schema_name
        and lt.table_name = tqs.table_name
    left join {{ ref('int_dbt__relations') }} as dm
        on lt.database_name = dm.database_name
        and lt.schema_name = dm.schema_name
        and lt.table_name = dm.table_name
),

final as (
    select
        current_timestamp() as analyzed_at,
        current_date() as snapshot_date,
        md5(
            cast(current_date() as string) || '|' || coalesce(table_fqn, '')
        ) as liquid_clustering_candidates_snapshot_key,
        database_name,
        schema_name,
        table_name,
        table_fqn,
        dbt_model,
        table_type,
        (
            case
                when select_count > 0
                then (select_count * (avg_execution_time_ms / 1000))
                    + (select_count / greatest(dml_count, 1)) * 10
                else 0
            end
        ) * (
            case
                when avg_file_size_mb is not null and avg_file_size_mb < {{ max_avg_file_size_mb }}
                then ({{ max_avg_file_size_mb }} / greatest(avg_file_size_mb, 1))
                else 1
            end
        ) as score,
        case
            when select_count > 0
                and (select_count / greatest(dml_count, 1)) > 1
                and size_gb >= {{ min_size_gb }}
                and (avg_file_size_mb is null or avg_file_size_mb < {{ max_avg_file_size_mb }})
            then true
            else false
        end as is_candidate,
        size_gb as table_size_gb,
        row_count as total_rows,
        file_count as current_file_count,
        avg_file_size_mb,
        is_already_clustered,
        avg_files_scanned,
        select_count,
        dml_count,
        round(select_count / greatest(dml_count, 1), 1) as query_to_dml_ratio,
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
    liquid_clustering_candidates_snapshot_key,
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
    current_file_count,
    avg_file_size_mb,
    is_already_clustered,
    avg_files_scanned,
    select_count,
    dml_count,
    query_to_dml_ratio,
    avg_query_duration_s
from final
{% if is_incremental() %}
where snapshot_date >= (
    select coalesce(max(snapshot_date), cast('1970-01-01' as date))
    from {{ this }}
)
{% endif %}
