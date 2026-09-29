{#-
  fct_snowflake__warehouse_performance_recommendations needs table-level attribution
  (Enterprise edition). This project runs the package's Standard edition path
  (snowflake_enterprise_edition: false), where the model must return no rows.
  Returns rows only on mismatch.
-#}
select * from {{ ref('fct_snowflake__warehouse_performance_recommendations') }}
