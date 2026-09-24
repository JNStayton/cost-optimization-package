{#--
  Dispatcher for find_table_materialization_candidates. Platform implementations:
    - snowflake__find_table_materialization_candidates   (macros/platforms/snowflake/optimizations/snowflake__find_table_materialization_candidates.sql)
  Add a <platform>__find_table_materialization_candidates with the same arguments to support another platform.
--#}
{% macro find_table_materialization_candidates(lookback_days=14, min_query_count=10) %}
  {{ return(adapter.dispatch('find_table_materialization_candidates', 'dbt_cost_optimization_package')(
      lookback_days=lookback_days,
      min_query_count=min_query_count
  )) }}
{% endmacro %}

{% macro default__find_table_materialization_candidates(lookback_days=14, min_query_count=10) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "find_table_materialization_candidates is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
