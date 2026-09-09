{{
  config(
    materialized='view',
    enabled=(target.type == 'databricks')
  )
}}

{% set lookback_days = var('cluster_key_suggestion_lookback_days', 30) %}

with table_columns as (
    select
        c.catalog_name,
        c.schema_name,
        c.table_name,
        c.column_name,
        c.data_type,
        c.ordinal_position
    from {{ ref('stg_databricks__columns') }} as c
    inner join {{ ref('int_databricks__table_inventory') }} as ti
        on c.catalog_name = ti.database_name
        and c.schema_name = ti.schema_name
        and c.table_name = ti.table_name
    -- Short column names (e.g. "id", "dt") are too likely to produce false positives
    -- via substring matching against query text
    where length(c.column_name) >= 4
),

recent_select_queries as (
    select
        query_id,
        query_text
    from {{ ref('int_databricks__query_history') }}
    where execution_status = 'SUCCESS'
        and statement_type = 'SELECT'
        and query_start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),

-- Queries that mention each table (via fuzzy text match on table name)
table_query_matches as (
    select distinct
        tc.catalog_name,
        tc.schema_name,
        tc.table_name,
        rq.query_id,
        rq.query_text
    from (
        select distinct catalog_name, schema_name, table_name
        from table_columns
    ) as tc
    inner join recent_select_queries as rq
        on rq.query_text ilike '%' || tc.table_name || '%'
),

table_query_totals as (
    select
        catalog_name,
        schema_name,
        table_name,
        count(*) as total_queries_for_table
    from table_query_matches
    group by 1, 2, 3
),

-- For each column, count how many of its table's queries also mention the column name
column_query_matches as (
    select
        tc.catalog_name,
        tc.schema_name,
        tc.table_name,
        tc.column_name,
        tc.data_type,
        tc.ordinal_position,
        count(
            case when tqm.query_text ilike '%' || tc.column_name || '%' then 1 end
        ) as query_match_count
    from table_columns as tc
    left join table_query_matches as tqm
        on tc.catalog_name = tqm.catalog_name
        and tc.schema_name = tqm.schema_name
        and tc.table_name = tqm.table_name
    group by 1, 2, 3, 4, 5, 6
),

scored as (
    select
        cqm.catalog_name,
        cqm.schema_name,
        cqm.table_name,
        cqm.column_name,
        cqm.data_type,
        cqm.ordinal_position,
        cqm.query_match_count,
        coalesce(tqt.total_queries_for_table, 0) as total_queries_for_table,
        case
            when coalesce(tqt.total_queries_for_table, 0) > 0
            then round(cqm.query_match_count * 100.0 / tqt.total_queries_for_table, 1)
            else 0
        end as match_rate_pct,
        -- Query frequency: dominant signal when history exists
        (cqm.query_match_count * 10)
        -- Type heuristic: date/timestamp columns make excellent clustering keys
        + case
            when lower(cqm.data_type) in ('date', 'timestamp', 'timestamp_ntz', 'timestamp_ltz')
            then 5
            else 0
          end
        -- Name heuristic: common filter/partition column naming conventions
        + case
            when lower(cqm.column_name) rlike
                '(date|_at|_time|created|updated|modified|region|country|state|status|type|category|partition|period)'
            then 3
            else 0
          end
        as total_score
    from column_query_matches as cqm
    left join table_query_totals as tqt
        on cqm.catalog_name = tqt.catalog_name
        and cqm.schema_name = tqt.schema_name
        and cqm.table_name = tqt.table_name
),

ranked as (
    select
        *,
        row_number() over (
            partition by catalog_name, schema_name, table_name
            order by total_score desc, ordinal_position asc
        ) as column_rank
    from scored
    where total_score > 0
)

select
    catalog_name,
    schema_name,
    table_name,
    column_name                   as suggested_cluster_key,
    data_type                     as suggested_column_data_type,
    query_match_count,
    total_queries_for_table,
    match_rate_pct,
    case
        when total_queries_for_table = 0 then 'HEURISTIC'
        when match_rate_pct >= 50        then 'HIGH'
        when match_rate_pct >= 20        then 'MEDIUM'
        else                                  'LOW'
    end as suggested_cluster_key_confidence
from ranked
where column_rank = 1
