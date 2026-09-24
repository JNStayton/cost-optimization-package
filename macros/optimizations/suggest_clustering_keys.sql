{#--
  Dispatcher for suggest_clustering_keys. Platform implementations:
    - snowflake__suggest_clustering_keys   (macros/snowflake/optimizations/suggest_clustering_keys.sql)
  Add a <platform>__suggest_clustering_keys with the same arguments to support another platform.
--#}
{% macro suggest_clustering_keys(model_name, database=none, schema=none, include_boolean_cols=false) %}
  {{ return(adapter.dispatch('suggest_clustering_keys', 'dbt_cost_optimization_package')(
      model_name=model_name,
      database=database,
      schema=schema,
      include_boolean_cols=include_boolean_cols
  )) }}
{% endmacro %}

{% macro default__suggest_clustering_keys(model_name, database=none, schema=none, include_boolean_cols=false) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "suggest_clustering_keys is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
