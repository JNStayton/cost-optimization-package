{#--
  Column-level clustering key recommendations for tables identified as candidates
  by fct_snowflake__table_clustering_candidates.

  Scoped to the top N candidates by score (clustering_key_cardinality_table_limit).
  One row per (snapshot_date, table_fqn, column_name).

  Build order guarantees correctness: since this model refs
  fct_snowflake__table_clustering_candidates, dbt builds that model first — including
  its post-hook, which runs refresh_column_cardinality and populates
  int_snowflake__column_cardinality before this model starts. Real cardinality is
  always available by the time this model runs.

  Scoring: (distinct_values / total_rows * 100) + (usage_count * 20)
  Columns with null cardinality (not included in macro pre-filter) score on
  usage_count only. recommended_key_position ranks columns within each table —
  positions 1–3 are flagged as is_recommended.
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
    -- exclude data types not supported as Snowflake clustering keys
    where tc.data_type not in ('VARIANT', 'ARRAY', 'OBJECT', 'GEOGRAPHY', 'GEOMETRY')
),

{% if use_access_history %}
column_usage as (
    select
        table_fqn,
        column_name,
        sum(query_count) as usage_count
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
        coalesce(cu.usage_count, 0) as usage_count,
        {% else %}
        0 as usage_count,
        {% endif %}
        case
            when tc.distinct_values is not null and tc.cardinality_total_rows > 0
                then (tc.distinct_values::float / tc.cardinality_total_rows) * 100
            else null
        end as cardinality_pct,
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
        coalesce(cardinality_pct, 0) + (usage_count * 20) as column_score
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
        cardinality_pct,
        cardinality_calculated_at,
        -- usage
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
    recommended_key_position <= 3 as is_recommended,
    column_score,
    -- cardinality
    distinct_values,
    cardinality_pct,
    cardinality_calculated_at,
    -- usage
    usage_count
from final
{% if is_incremental() %}
where snapshot_date >= (
    select coalesce(max(snapshot_date), '1970-01-01'::date)
    from {{ this }}
)
{% endif %}
