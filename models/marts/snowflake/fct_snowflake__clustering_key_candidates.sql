{#--
  Column-level clustering key recommendations for tables identified as candidates
  by fct_snowflake__table_clustering_candidates.

  Scoped to the top N candidates by score (clustering_key_cardinality_table_limit).
  One row per (snapshot_date, table_fqn, column_name) for the top 3 recommended
  columns per table only.

  Build order guarantees correctness: since this model refs
  fct_snowflake__table_clustering_candidates, dbt builds that model first — including
  its post-hook, which runs refresh_column_cardinality and populates
  int_snowflake__column_cardinality before this model starts. Real cardinality is
  always available by the time this model runs.

  Scoring: (avg_rows_per_value / total_rows * 100) + ((where_query_count + join_query_count) * 20)
  avg_rows_per_value = total_rows / distinct_values. Higher values mean fewer distinct
  values relative to rows — lower cardinality — which is better for micropartition
  co-location. Columns with null cardinality score on usage only.

  Cardinality guardrails (applied when cardinality data is available):
    - distinct_values > 10:            excludes near-boolean columns
    - distinct_values < total_rows:    excludes unique/near-unique keys

  Usage signal (Enterprise+ only): where_query_count and join_query_count from
  int_snowflake__column_query_stats, matched against query_text WHERE and JOIN...ON
  patterns. Standard edition scores on cardinality only (usage = 0).

  Known limitation: column aliasing in query_text is not detected.
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
            or (cc.distinct_values > 10 and cc.distinct_values < cc.total_rows)
        )
),

{% if use_access_history %}
column_usage as (
    select
        table_fqn,
        column_name,
        sum(where_query_count) as where_query_count,
        sum(join_query_count)  as join_query_count
    from {{ ref('int_snowflake__column_query_stats') }}
    where access_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by table_fqn, column_name
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
        {% if use_access_history %}
        coalesce(cu.where_query_count, 0) as where_query_count,
        coalesce(cu.join_query_count, 0)  as join_query_count,
        {% else %}
        0 as where_query_count,
        0 as join_query_count,
        {% endif %}
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
    {% if use_access_history %}
    left join column_usage as cu
        on tc.table_fqn = cu.table_fqn
        and tc.column_name = cu.column_name
    {% endif %}
),

column_scored as (
    select
        *,
        where_query_count + join_query_count as usage_count,
        coalesce(avg_rows_per_value / nullif(cardinality_total_rows, 0) * 100, 0)
            + (where_query_count + join_query_count) * 20 as column_score
    from scored
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
