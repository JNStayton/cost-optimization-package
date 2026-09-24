{#--
  Dispatcher for find_incremental_materialization_candidates. Platform implementations:
    - snowflake__find_incremental_materialization_candidates   (macros/platforms/snowflake/optimizations/snowflake__find_incremental_materialization_candidates.sql)
  Add a <platform>__find_incremental_materialization_candidates with the same arguments to support another platform.
--#}
{% macro find_incremental_materialization_candidates(min_table_size_gb=10, max_build_time_sec=600, lookback_days=30) %}
  {{ return(adapter.dispatch('find_incremental_materialization_candidates', 'dbt_cost_optimization_package')(
      min_table_size_gb=min_table_size_gb,
      max_build_time_sec=max_build_time_sec,
      lookback_days=lookback_days
  )) }}
{% endmacro %}

{% macro default__find_incremental_materialization_candidates(min_table_size_gb=10, max_build_time_sec=600, lookback_days=30) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "find_incremental_materialization_candidates is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
