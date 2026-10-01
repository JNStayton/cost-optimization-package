{#-
  Clustering key evidence counts a column's filters only from queries that scan the
  candidate table itself (the same queries as total_queries_analyzed), and matches column
  names whole:
    - The hook analyzed 3 reads of daily_demo_events (a child TABLE of demo_events) and 7
      reads of demo_events. The child reads filter EVENT_DATE, but never scan demo_events,
      so they must not count: EVENT_DATE has 7 filtering queries of 7 analyzed (the old
      logic gave 10 of 7, a filter share above 1).
    - The demo_events reads filter EVENT_ID; ID (a column name contained in EVENT_ID) must
      get no filter evidence and must not become a clustering key.
    - No key candidate's filter count exceeds its analyzed count.
  Returns one row per failed check.
-#}
{%- set events_fqn = (target.database ~ '.' ~ target.schema ~ '.demo_events') | upper %}

with keys as (
    select * from {{ ref('fct_snowflake__clustering_key_candidates') }}
    where snapshot_date = (select max(snapshot_date) from {{ ref('fct_snowflake__clustering_key_candidates') }})
),

evidence as (
    select * from {{ ref('int_snowflake__query_operator_evidence') }}
    where table_fqn = '{{ events_fqn }}'
),

checks as (
    select 'fixture: child-table queries have filter evidence but no scan' as check_name,
           (select count(distinct query_id) from evidence
            where operator_type = 'Filter'
              and query_id not in (select query_id from evidence where operator_type = 'TableScan'))::varchar as produced,
           '3' as expected
    union all
    select 'EVENT_DATE: filtering queries / analyzed',
           (select filter_query_count || ' / ' || total_queries_analyzed from keys
            where table_fqn = '{{ events_fqn }}' and upper(column_name) = 'EVENT_DATE'),
           '7 / 7'
    union all
    select 'no key candidate has more filters than analyzed queries',
           (select count(*) from keys where filter_query_count > total_queries_analyzed)::varchar,
           '0'
    union all
    select 'fixture: EVENT_ID filters were captured',
           (select (count(distinct query_id) > 0)::varchar from evidence
            where operator_type = 'Filter' and column_name = 'EVENT_ID'),
           'true'
    union all
    select 'ID gets no filter evidence',
           (select count(*) from evidence where operator_type = 'Filter' and column_name = 'ID')::varchar,
           '0'
    union all
    select 'ID is not a key candidate',
           (select count(*) from keys where table_fqn = '{{ events_fqn }}' and upper(column_name) = 'ID')::varchar,
           '0'
)

select * from checks where produced is distinct from expected
