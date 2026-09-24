{#--
  ON-clause fragment for joining a BigQuery relation (aliased `left_alias`, with
  database_name/schema_name/table_name columns) to int_dbt__relations as `dm`.

  int_dbt__relations always upper()s identifiers (a Snowflake convention); BigQuery
  identifiers are case-sensitive and typically lowercase, so this compares
  case-insensitively rather than assuming a case convention. Without it, the join
  silently matches zero rows for any BigQuery table whose real name isn't already
  uppercase — see int_bigquery__table_query_stats_daily and
  fct_bigquery__table_clustering_candidates, the two call sites.
--#}
{% macro bigquery__dbt_relations_case_insensitive_join(left_alias) %}
    on upper({{ left_alias }}.database_name) = dm.database_name
    and upper({{ left_alias }}.schema_name) = dm.schema_name
    and upper({{ left_alias }}.table_name) = dm.table_name
{% endmacro %}
