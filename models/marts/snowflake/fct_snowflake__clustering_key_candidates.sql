{#--
  Top 3 clustering key recommendations per candidate table, scored on WHERE/JOIN
  query usage and cardinality. Requires fct_snowflake__table_clustering_candidates
  to build first — its post-hook populates int_snowflake__column_cardinality before
  this model runs. Known limitation: column aliasing in query_text is not detected.
--#}
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='clustering_key_candidate_snapshot_key',
    enabled=(target.type == 'snowflake')
  )
}}

{% set lookback_days = var('clustering_candidates_lookback_days', 7) %}
{% set cardinality_limit = var('clustering_key_cardinality_table_limit', 10) %}
{% set use_access_history = var('use_access_history_attribution', true) %}

with candidates as (
    select
        table_fqn,
        database_name,
        schema_name,
        table_name,
        dbt_model,
        score as table_score
    from {{ ref('fct_snowflake__table_clustering_candidates') }}
    where is_candidate = true
        and snapshot_date = (
            select max(snapshot_date)
            from {{ ref('fct_snowflake__table_clustering_candidates') }}
        )
    qualify row_number() over (order by score desc) <= {{ cardinality_limit }}
),

table_columns as (
    select
        tc.table_fqn,
        tc.column_name,
        tc.ordinal_position,
        tc.data_type,
        cc.distinct_values,
        cc.total_rows as cardinality_total_rows,
        cc.calculated_at as cardinality_calculated_at
    from {{ ref('int_snowflake__table_columns') }} as tc
    inner join candidates as c
        on tc.table_fqn = c.table_fqn
    left join {{ ref('int_snowflake__column_cardinality') }} as cc
        on tc.table_fqn = cc.table_fqn
        and tc.column_name = cc.column_name
    where tc.data_type not in ('VARIANT', 'ARRAY', 'OBJECT', 'GEOGRAPHY', 'GEOMETRY')
        and (
            cc.distinct_values is null
            or cc.distinct_values < cc.total_rows * 0.5
        )
),

{% if use_access_history %}
column_usage as (
    -- Enterprise+: ACCESS_HISTORY provides precise column→query attribution;
    -- query_text matching is scoped to only queries that actually accessed the column.
    select
        table_fqn,
        column_name,
        sum(where_query_count) as where_query_count,
        sum(join_query_count)  as join_query_count
    from {{ ref('int_snowflake__column_query_stats') }}
    where access_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by table_fqn, column_name
),
{% else %}
column_usage as (
    -- Standard: no ACCESS_HISTORY, so scope relevant queries by table name in
    -- query_text, then apply WHERE/JOIN column matching against that subset.
    select
        tc.table_fqn,
        tc.column_name,
        count(distinct case
            when qh.query_text ilike '%WHERE%' || tc.column_name || '%'
                then qh.query_id
        end) as where_query_count,
        count(distinct case
            when qh.query_text ilike '%JOIN%'
                and qh.query_text ilike '%ON%' || tc.column_name || '%'
                then qh.query_id
        end) as join_query_count
    from table_columns as tc
    inner join {{ ref('int_snowflake__query_history') }} as qh
        on qh.query_text ilike '%' || split_part(tc.table_fqn, '.', 3) || '%'
    where qh.query_start_time >= dateadd(day, -{{ lookback_days }}, current_date())
    group by tc.table_fqn, tc.column_name
),
{% endif %}

scored as (
    select
        tc.table_fqn,
        tc.column_name,
        tc.ordinal_position,
        tc.data_type,
        tc.distinct_values,
        tc.cardinality_total_rows,
        tc.cardinality_calculated_at,
        coalesce(cu.where_query_count, 0) as where_query_count,
        coalesce(cu.join_query_count, 0)  as join_query_count,
        case
            when tc.distinct_values is not null and tc.distinct_values > 0
                then tc.cardinality_total_rows::float / tc.distinct_values
            else null
        end as avg_rows_per_value,
        c.table_score,
        c.dbt_model
    from table_columns as tc
    inner join candidates as c
        on tc.table_fqn = c.table_fqn
    left join column_usage as cu
        on tc.table_fqn = cu.table_fqn
        and tc.column_name = cu.column_name
),

column_scored as (
    select
        *,
        where_query_count + join_query_count as usage_count,
        coalesce(avg_rows_per_value / nullif(cardinality_total_rows, 0) * 100, 0)
            + (where_query_count + join_query_count) * 20 as column_score
    from scored
    where where_query_count + join_query_count > 0
),

final as (
    select
        -- snapshot metadata
        md5(
            to_varchar(current_date()) || '|' || coalesce(table_fqn, '') || '|' || coalesce(column_name, '')
        ) as clustering_key_candidate_snapshot_key,
        current_date()      as snapshot_date,
        current_timestamp() as analyzed_at,
        -- table identity
        table_fqn,
        split_part(table_fqn, '.', 1) as database_name,
        split_part(table_fqn, '.', 2) as schema_name,
        split_part(table_fqn, '.', 3) as table_name,
        dbt_model,
        -- column
        column_name,
        ordinal_position,
        data_type,
        -- recommendation
        row_number() over (
            partition by table_fqn
            order by column_score desc
        ) as recommended_key_position,
        -- scoring
        column_score,
        -- cardinality
        distinct_values,
        avg_rows_per_value,
        cardinality_calculated_at,
        -- usage
        where_query_count,
        join_query_count,
        usage_count
    from column_scored
)

select
    -- snapshot metadata
    clustering_key_candidate_snapshot_key,
    snapshot_date,
    analyzed_at,
    -- table identity
    table_fqn,
    database_name,
    schema_name,
    table_name,
    dbt_model,
    -- column
    column_name,
    ordinal_position,
    data_type,
    -- recommendation
    recommended_key_position,
    -- scoring
    column_score,
    -- cardinality
    distinct_values,
    avg_rows_per_value,
    cardinality_calculated_at,
    -- usage
    where_query_count,
    join_query_count,
    usage_count
from final
where recommended_key_position <= 3
{% if is_incremental() %}
    and snapshot_date >= (
        select coalesce(max(snapshot_date), '1970-01-01'::date)
        from {{ this }}
    )
{% endif %}
