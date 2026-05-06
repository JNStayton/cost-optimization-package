{{
  config(
    materialized='view',
    enabled=(target.type == 'bigquery' and var('use_query_text_attribution', true))
  )
}}

{#--
  BigQuery column-level access attribution from query text. Heuristic — BigQuery has
  no engine-attested column lineage in JOBS_BY_PROJECT (no equivalent of Snowflake's
  ACCESS_HISTORY.columns[]). We extract substrings rooted at filter contexts (WHERE,
  JOIN ON, ORDER BY) from query_text, then check column-name presence with a
  word-boundary-aware regex.

  Caveats:
    - Cannot resolve aliases or CTEs; matches by raw column name within filter substrings.
    - May over-match when a column name appears inside a comment inside a filter clause.
    - Word-boundary matching prevents 'id' from matching 'customer_id'.

  Lookback window: var('clustering_candidates_lookback_days', default 7).
--#}

{% set lookback_days = var('clustering_candidates_lookback_days', 7) %}

with query_text_window as (
    select
        query_id,
        query_start_time,
        query_text
    from {{ ref('int_bigquery__query_history') }}
    where execution_status = 'SUCCESS'
        and query_start_time >= timestamp_sub(current_timestamp(), interval {{ lookback_days }} day)
),

candidate_tables as (
    -- distinct (table_fqn → table_name) pairs for substring-match attribution
    select distinct
        table_fqn,
        database_name as table_database,
        schema_name as table_schema,
        table_name
    from {{ ref('int_bigquery__table_columns') }}
),

queries_to_tables as (
    select
        qtw.query_id,
        qtw.query_start_time,
        qtw.query_text,
        ct.table_fqn,
        ct.table_database,
        ct.table_schema,
        ct.table_name
    from query_text_window as qtw
    inner join candidate_tables as ct
        -- mirrors the table-text matcher in int_bigquery__table_query_stats_daily
        on lower(qtw.query_text) like '%' || lower(ct.table_name) || '%'
),

-- Concatenate every WHERE / JOIN-ON / ORDER BY substring into a single haystack per
-- (query_id, table_fqn). REGEXP_EXTRACT_ALL with non-greedy capture; we union the
-- three context types and array-concatenate so the column match below sees only
-- filter-context content.
filter_contexts as (
    select
        query_id,
        query_start_time,
        table_fqn,
        table_database,
        table_schema,
        table_name,
        array_to_string(
            array_concat(
                regexp_extract_all(query_text, r'(?is)\bWHERE\b(.*?)(?:\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|\bUNION\b|\bHAVING\b|;|$)'),
                regexp_extract_all(query_text, r'(?is)\bJOIN\b[^()]*?\bON\b(.*?)(?:\bWHERE\b|\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|\bUNION\b|\bJOIN\b|;|$)'),
                regexp_extract_all(query_text, r'(?is)\bORDER\s+BY\b(.*?)(?:\bLIMIT\b|\bUNION\b|;|$)')
            ),
            ' '
        ) as filter_haystack
    from queries_to_tables
),

attributed as (
    select distinct
        fc.query_id,
        fc.query_start_time,
        fc.table_fqn,
        fc.table_database,
        fc.table_schema,
        fc.table_name,
        tc.column_name
    from filter_contexts as fc
    inner join {{ ref('int_bigquery__table_columns') }} as tc
        on fc.table_fqn = tc.table_fqn
    -- Word-boundary match: prevents 'id' from matching 'customer_id'.
    -- BigQuery RE2 supports \b. We lower-case both sides for case insensitivity.
    where regexp_contains(
        lower(fc.filter_haystack),
        r'(?i)\b' || lower(tc.column_name) || r'\b'
    )
)

select
    query_id,
    query_start_time,
    table_fqn,
    table_database,
    table_schema,
    table_name,
    column_name
from attributed
