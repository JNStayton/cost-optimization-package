{#--
  Dispatcher for get_clustering_score. Platform implementations:
    - snowflake__get_clustering_score   (macros/snowflake/optimizations/helpers/snowflake__get_clustering_score.sql)
  Add a <platform>__get_clustering_score with the same arguments to support another platform.
--#}
{% macro get_clustering_score(avg_rows, total_rows, usage_count) %}
  {{ return(adapter.dispatch('get_clustering_score', 'dbt_cost_optimization_package')(
      avg_rows=avg_rows,
      total_rows=total_rows,
      usage_count=usage_count
  )) }}
{% endmacro %}

{% macro default__get_clustering_score(avg_rows, total_rows, usage_count) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "get_clustering_score is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
