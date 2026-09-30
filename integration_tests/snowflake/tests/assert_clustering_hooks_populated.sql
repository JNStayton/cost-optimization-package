{#-
  The two post-hooks on fct_snowflake__table_clustering_candidates wrote what they should
  for demo_events:
    - extract_operator_evidence analyzed the 10 most recent of the 20 real fixture SELECTs (clustering_key_operator_queries_per_table: 10) (via
      GET_QUERY_OPERATOR_STATS) and captured Filter operators on EVENT_DATE and REGION.
    - refresh_column_cardinality recorded exact distinct counts for the low-cardinality
      columns. (CUSTOMER_ID and EVENT_ID are left out: their counts are approximate.)
  Returns one row per failed check.
-#}
-- The hooks run on this model, so the test must run after it, not just after the tables it reads.
-- depends_on: {{ ref('fct_snowflake__table_clustering_candidates') }}
{%- set events_fqn = (target.database ~ '.' ~ target.schema ~ '.demo_events') | upper %}

with evidence as (
    select * from {{ ref('int_snowflake__query_operator_evidence') }}
    where table_fqn = '{{ events_fqn }}'
),

checks as (
    select 'evidence: distinct query IDs' as check_name,
           (select count(distinct query_id) from evidence) as produced, 10 as expected
    union all
    select 'evidence: queries with a Filter on EVENT_DATE',
           (select count(distinct query_id) from evidence where operator_type = 'Filter' and column_name = 'EVENT_DATE'), 10
    union all
    select 'evidence: queries with a Filter on REGION',
           (select count(distinct query_id) from evidence where operator_type = 'Filter' and column_name = 'REGION'), 10
{%- for col, n in [('EVENT_DATE', 30), ('REGION', 5), ('IS_TEST', 2)] %}
    union all
    select 'cardinality: {{ col }}',
           (select max(distinct_values) from {{ ref('int_snowflake__column_cardinality') }}
            where table_fqn = '{{ events_fqn }}' and upper(column_name) = '{{ col }}'), {{ n }}
{%- endfor %}
)

select * from checks where produced is distinct from expected
