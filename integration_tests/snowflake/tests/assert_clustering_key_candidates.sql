{#-
  fct_snowflake__clustering_key_candidates recommends clustering demo_events on EVENT_DATE,
  then REGION: the two columns every fixture query filters on. Columns that are never
  filtered (EVENT_ID, CUSTOMER_ID, IS_TEST, AMOUNT) aren't recommended.
  Returns rows only on mismatch.
-#}
with produced as (
    select lower(table_name) as table_name, upper(column_name) as column_name, recommended_key_position
    from {{ ref('fct_snowflake__clustering_key_candidates') }}
    where startswith(lower(table_name), 'demo_')
),

expected as (
    select 'demo_events' as table_name, 'EVENT_DATE' as column_name, 1 as recommended_key_position
    union all select 'demo_events', 'REGION', 2
)

select
    coalesce(p.table_name, e.table_name)   as table_name,
    coalesce(p.column_name, e.column_name) as column_name,
    p.recommended_key_position as produced_position,
    e.recommended_key_position as expected_position
from produced as p
full outer join expected as e
    on p.table_name = e.table_name and p.column_name = e.column_name
where p.recommended_key_position is distinct from e.recommended_key_position
