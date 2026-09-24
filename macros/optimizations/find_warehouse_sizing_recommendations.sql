{#--
  Dispatcher for find_warehouse_sizing_recommendations. Platform implementations:
    - snowflake__find_warehouse_sizing_recommendations   (macros/platforms/snowflake/optimizations/snowflake__find_warehouse_sizing_recommendations.sql)
  Add a <platform>__find_warehouse_sizing_recommendations with the same arguments to support another platform.
--#}
{% macro find_warehouse_sizing_recommendations(lookback_days=7, min_query_count=20, dml_threshold=0.35) %}
  {{ return(adapter.dispatch('find_warehouse_sizing_recommendations', 'dbt_cost_optimization_package')(
      lookback_days=lookback_days,
      min_query_count=min_query_count,
      dml_threshold=dml_threshold
  )) }}
{% endmacro %}

{% macro default__find_warehouse_sizing_recommendations(lookback_days=7, min_query_count=20, dml_threshold=0.35) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "find_warehouse_sizing_recommendations is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
