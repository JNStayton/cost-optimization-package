{#--
  Dispatcher for get_clustering_cardinality_stats. Platform implementations:
    - snowflake__get_clustering_cardinality_stats   (macros/snowflake/optimizations/helpers/get_clustering_cardinality_stats.sql)
  Add a <platform>__get_clustering_cardinality_stats with the same arguments to support another platform.
--#}
{% macro get_clustering_cardinality_stats(model_relation, include_boolean_cols=false) %}
  {{ return(adapter.dispatch('get_clustering_cardinality_stats', 'dbt_cost_optimization_package')(
      model_relation=model_relation,
      include_boolean_cols=include_boolean_cols
  )) }}
{% endmacro %}

{% macro default__get_clustering_cardinality_stats(model_relation, include_boolean_cols=false) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "get_clustering_cardinality_stats is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
