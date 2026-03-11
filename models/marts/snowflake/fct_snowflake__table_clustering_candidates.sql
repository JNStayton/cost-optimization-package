{#--
  Historical snapshot of clustering candidates based on Snowflake metadata and query history.
  This model ports the logic from the find_table_clustering_candidates macro into SQL.
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

with
large_tables as (
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
        iff(
            coalesce(sum(tqs.select_count), 0) > 0,
            coalesce(sum(tqs.select_execution_time_ms_sum), 0) / nullif(sum(tqs.select_count), 0),
            0
        ) as avg_execution_time_ms,
        iff(
            coalesce(sum(tqs.select_count), 0) > 0,
            coalesce(sum(tqs.select_partitions_scanned_sum), 0) / nullif(sum(tqs.select_count), 0),
            0
        ) as avg_partitions_scanned,
        iff(
            coalesce(sum(tqs.select_count), 0) > 0,
            coalesce(sum(tqs.select_partitions_total_sum), 0) / nullif(sum(tqs.select_count), 0),
            0
        ) as avg_partitions_total
    from large_tables as lt
    left join {{ ref('int_table_query_stats_daily') }} as tqs
        on upper(lt.database_name) = upper(tqs.table_database)
        and upper(lt.schema_name) = upper(tqs.table_schema)
        and upper(lt.table_name) = upper(tqs.table_name)
        and tqs.stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
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
        lt.size_gb,
        coalesce(lt.row_count, 0) as row_count,
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
        md5(
            to_varchar(current_date()) || '|' || coalesce(table_fqn, '')
        ) as clustering_candidates_snapshot_key,
        database_name,
        schema_name,
        table_name,
        table_fqn,
        dbt_model,
        table_type,
        (
            case
                when select_count > 0 then
                    (select_count * (avg_execution_time_ms / 1000))
                    + ((select_count / iff(dml_count = 0, 1, dml_count)) * 10)
                else 0
            end
        )
        * (
            case
                when row_count > 0 and (micropartitions / row_count) * 100 > 0.0001
                    then (micropartitions / row_count) * 100
                else 1
            end
        ) as score,
        case
            when
                select_count > 0
                and (select_count / iff(dml_count = 0, 1, dml_count)) > 1
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
    select coalesce(max(snapshot_date), '1970-01-01'::date)
    from {{ this }}
)
{% endif %}
