{#--
  Dispatcher for find_table_clustering_candidates. Platform implementations:
    - snowflake__find_table_clustering_candidates   (macros/platforms/snowflake/optimizations/snowflake__find_table_clustering_candidates.sql)
  Add a <platform>__find_table_clustering_candidates with the same arguments to support another platform.
--#}
{% macro find_table_clustering_candidates(lookback_days=7, ignore_table_size=false, dbt_project_only=true, include_package_models=false, target_databases=[], target_schemas=[]) %}
  {{ return(adapter.dispatch('find_table_clustering_candidates', 'dbt_cost_optimization_package')(
      lookback_days=lookback_days,
      ignore_table_size=ignore_table_size,
      dbt_project_only=dbt_project_only,
      include_package_models=include_package_models,
      target_databases=target_databases,
      target_schemas=target_schemas
  )) }}
{% endmacro %}

{% macro default__find_table_clustering_candidates(lookback_days=7, ignore_table_size=false, dbt_project_only=true, include_package_models=false, target_databases=[], target_schemas=[]) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "find_table_clustering_candidates is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
