{#--
  get_clustering_score returns (avg_rows / total_rows) * 100 + usage_count * 20,
  or 0 when total_rows is 0. Inputs arrive as text from query results, so they are
  cast; text and numeric inputs must score the same.
  Returns rows only on mismatch.
--#}
with cases as (
    select 'cardinality plus usage' as case_name, {{ get_clustering_score(10, 1000, 3) }}       as got, 61  as expected
    union all select 'no usage',               {{ get_clustering_score(10, 1000, 0) }},          1
    union all select 'zero total_rows',        {{ get_clustering_score(10, 0, 5) }},             0
    union all select 'text inputs',            {{ get_clustering_score('10', '1000', '3') }},    61
    union all select 'decimal cardinality',    {{ get_clustering_score(250.5, 1000, 1) }},       45.05
)
select *
from cases
where abs(got - expected) > 0.000001
