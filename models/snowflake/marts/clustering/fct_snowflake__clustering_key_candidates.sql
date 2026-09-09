{#--
  Clustering key recommendations per candidate table, scored on confirmed
  Filter/Join operator usage from consumption queries only.

  Requires fct_snowflake__table_clustering_candidates to build first — its
  post-hooks populate int_snowflake__column_cardinality and
  int_snowflake__query_operator_evidence before this model runs.

  Gating (two thresholds):
    1. filter_query_count > 0 AND filter proportion >= 33% of analyzed consumption queries
       (join-only columns are excluded — filter is the admission ticket)
    2. column_score >= 50% of the table's top-scored column
       (prevents diminishing-returns keys from being recommended)

  Scoring formula:
    - filter_query_count * 3 (WHERE predicates = strongest clustering signal)
    - join_query_count * 1   (JOIN co-location = secondary benefit)
    - cardinality_bonus: +10 when avg_rows_per_value between 100 and 10000

  Only consumption queries contribute to column evidence. Build/test queries
  are excluded to avoid false positives from dbt test patterns or full-table
  model rebuilds.
--#}
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='clustering_key_candidate_snapshot_key',
  )
}}

{% set lookback_days = var('clustering_candidates_lookback_days', 7) %}
{% set cardinality_limit = var('clustering_key_cardinality_table_limit', 10) %}

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
    -- Regular columns
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

    union all

    -- Expression candidates: timestamp columns profiled as TO_DATE(col)
    select
        cc.table_fqn,
        cc.column_name,
        tc.ordinal_position,
        'DATE' as data_type,
        cc.distinct_values,
        cc.total_rows as cardinality_total_rows,
        cc.calculated_at as cardinality_calculated_at
    from {{ ref('int_snowflake__column_cardinality') }} as cc
    inner join candidates as c
        on cc.table_fqn = c.table_fqn
    inner join {{ ref('int_snowflake__table_columns') }} as tc
        on cc.table_fqn = tc.table_fqn
        and upper(tc.column_name) = upper(replace(replace(cc.column_name, 'to_date(', ''), ')', ''))
    where cc.column_name like 'to_date(%)'
        and cc.distinct_values < cc.total_rows * 0.5
),

column_usage as (
    -- Filter/Join evidence from consumption queries only
    select
        oe.table_fqn,
        oe.column_name,
        count(distinct case when oe.operator_type = 'Filter' then oe.query_id end) as filter_query_count,
        count(distinct case when oe.operator_type = 'Join' then oe.query_id end) as join_query_count
    from {{ ref('int_snowflake__query_operator_evidence') }} as oe
    inner join {{ ref('int_snowflake__query_workload_class') }} as wc
        on oe.query_id = wc.query_id
    where wc.workload_class = 'consumption'
      and oe.operator_type in ('Filter', 'Join')
      and oe.access_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by oe.table_fqn, oe.column_name

    union all

    -- Expression usage: map raw timestamp filter evidence to to_date(col) form
    select
        oe.table_fqn,
        'to_date(' || lower(oe.column_name) || ')' as column_name,
        count(distinct case when oe.operator_type = 'Filter' then oe.query_id end) as filter_query_count,
        count(distinct case when oe.operator_type = 'Join' then oe.query_id end) as join_query_count
    from {{ ref('int_snowflake__query_operator_evidence') }} as oe
    inner join {{ ref('int_snowflake__query_workload_class') }} as wc
        on oe.query_id = wc.query_id
    inner join {{ ref('int_snowflake__table_columns') }} as tc
        on oe.table_fqn = tc.table_fqn
        and oe.column_name = tc.column_name
    where wc.workload_class = 'consumption'
      and oe.operator_type in ('Filter', 'Join')
      and tc.data_type ilike 'TIMESTAMP%'
      and oe.access_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by oe.table_fqn, 'to_date(' || lower(oe.column_name) || ')'
),

total_queries_per_table as (
    -- Denominator: consumption queries with a confirmed TableScan on this FQN
    -- (not just queries that happened to match a Filter/Join)
    select
        oe.table_fqn,
        count(distinct oe.query_id) as total_queries_analyzed
    from {{ ref('int_snowflake__query_operator_evidence') }} as oe
    inner join {{ ref('int_snowflake__query_workload_class') }} as wc
        on oe.query_id = wc.query_id
    where wc.workload_class = 'consumption'
      and oe.operator_type = 'TableScan'
      and oe.access_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by oe.table_fqn
),

scored as (
    select
        tc.table_fqn,
        tc.column_name,
        tc.ordinal_position,
        tc.data_type,
        tc.distinct_values,
        tc.cardinality_total_rows,
        tc.cardinality_calculated_at,
        coalesce(cu.filter_query_count, 0) as filter_query_count,
        coalesce(cu.join_query_count, 0)   as join_query_count,
        coalesce(tqa.total_queries_analyzed, 0) as total_queries_analyzed,
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
    left join total_queries_per_table as tqa
        on tc.table_fqn = tqa.table_fqn
),

column_scored as (
    select
        *,
        filter_query_count + join_query_count as usage_count,
        -- Scoring: filter usage weighted 3x (direct pruning), join 1x (co-location)
        -- Cardinality bonus for sweet-spot grouping (100-10000 rows per value)
        (filter_query_count * 3)
            + (join_query_count * 1)
            + case
                when avg_rows_per_value between 100 and 10000 then 10
                when avg_rows_per_value between 10 and 100000 then 5
                else 0
              end as column_score
    from scored
    where filter_query_count > 0
        -- Proportion gate: column must be filtered on in >= 1/3 of analyzed queries
        and filter_query_count::float / nullif(total_queries_analyzed, 0) >= 0.33
        -- Selectivity gate: column must have low enough cardinality for clustering benefit
        -- High-selectivity columns (near-unique) can't consolidate into micropartitions
        and (
            distinct_values is null
            or cardinality_total_rows is null
            or cardinality_total_rows = 0
            or distinct_values::float / cardinality_total_rows <= 0.05
        )
),

final as (
    select
        md5(
            to_varchar(current_date()) || '|' || coalesce(table_fqn, '') || '|' || coalesce(column_name, '')
        ) as clustering_key_candidate_snapshot_key,
        current_date()      as snapshot_date,
        current_timestamp() as analyzed_at,
        table_fqn,
        split_part(table_fqn, '.', 1) as database_name,
        split_part(table_fqn, '.', 2) as schema_name,
        split_part(table_fqn, '.', 3) as table_name,
        dbt_model,
        column_name,
        ordinal_position,
        data_type,
        row_number() over (
            partition by table_fqn
            order by column_score desc
        ) as recommended_key_position,
        column_score,
        max(column_score) over (partition by table_fqn) as top_column_score,
        distinct_values,
        avg_rows_per_value,
        cardinality_calculated_at,
        filter_query_count,
        join_query_count,
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
    column_score,
    distinct_values,
    avg_rows_per_value,
    cardinality_calculated_at,
    filter_query_count,
    join_query_count,
    usage_count
from final
where recommended_key_position <= 3
    -- Diminishing returns gate: 2nd/3rd keys must be >= 50% as impactful as the top key
    and column_score::float / nullif(top_column_score, 0) >= 0.50
{% if is_incremental() %}
    and snapshot_date >= (
        select coalesce(max(snapshot_date), '1970-01-01'::date)
        from {{ this }}
    )
{% endif %}
