{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='clustering_key_candidate_snapshot_key'
  )
}}

{% set lookback_days = var('clustering_candidates_lookback_days', 7) %}
{% set cardinality_limit = var('clustering_key_cardinality_table_limit', 10) %}
{% set use_query_text_attribution = var('use_query_text_attribution', true) %}

with candidates as (
    select
        table_fqn,
        database_name,
        schema_name,
        table_name,
        dbt_model,
        score as table_score
    from {{ ref('fct_bigquery__table_clustering_candidates') }}
    where is_candidate = true
        and snapshot_date = (
            select max(snapshot_date)
            from {{ ref('fct_bigquery__table_clustering_candidates') }}
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
    from {{ ref('int_bigquery__table_columns') }} as tc
    inner join candidates as c
        on tc.table_fqn = c.table_fqn
    left join {{ ref('int_bigquery__column_cardinality') }} as cc
        on tc.table_fqn = cc.table_fqn
        and tc.column_name = cc.column_name
),

{% if use_query_text_attribution %}
column_usage as (
    select
        table_fqn,
        column_name,
        sum(query_count) as usage_count
    from {{ ref('int_bigquery__column_query_stats') }}
    where access_date >= date_sub(current_date(), interval {{ lookback_days }} day)
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
        {% if use_query_text_attribution %}
        coalesce(cu.usage_count, 0) as usage_count,
        {% else %}
        0 as usage_count,
        {% endif %}
        case
            when tc.distinct_values is not null and tc.cardinality_total_rows > 0
                then (cast(tc.distinct_values as float64) / tc.cardinality_total_rows) * 100
            else null
        end as cardinality_pct,
        c.table_score,
        c.dbt_model
    from table_columns as tc
    inner join candidates as c
        on tc.table_fqn = c.table_fqn
    {% if use_query_text_attribution %}
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
        to_hex(md5(
            cast(current_date() as string) || '|' || coalesce(table_fqn, '') || '|' || coalesce(column_name, '')
        )) as clustering_key_candidate_snapshot_key,
        current_date()      as snapshot_date,
        current_timestamp() as analyzed_at,
        table_fqn,
        split(table_fqn, '.')[safe_offset(0)] as database_name,
        split(table_fqn, '.')[safe_offset(1)] as schema_name,
        split(table_fqn, '.')[safe_offset(2)] as table_name,
        dbt_model,
        column_name,
        ordinal_position,
        data_type,
        row_number() over (
            partition by table_fqn
            order by column_score desc
        ) as recommended_key_position,
        column_score,
        distinct_values,
        cardinality_pct,
        cardinality_calculated_at,
        usage_count
    from column_scored
)

select
    clustering_key_candidate_snapshot_key,
    snapshot_date,
    analyzed_at,
    table_fqn,
    database_name,
    schema_name,
    table_name,
    dbt_model,
    column_name,
    ordinal_position,
    data_type,
    recommended_key_position,
    recommended_key_position <= 4 as is_recommended,
    column_score,
    distinct_values,
    cardinality_pct,
    cardinality_calculated_at,
    usage_count
from final
{% if is_incremental() %}
where snapshot_date >= (
    select coalesce(max(snapshot_date), date('1970-01-01'))
    from {{ this }}
)
{% endif %}
