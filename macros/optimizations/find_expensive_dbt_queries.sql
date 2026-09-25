{#--
  Dispatcher for find_expensive_dbt_queries. Platform implementations:
    - snowflake__find_expensive_dbt_queries   (macros/platforms/snowflake/optimizations/snowflake__find_expensive_dbt_queries.sql)
  Add a <platform>__find_expensive_dbt_queries with the same arguments to support another platform.
--#}
{% macro find_expensive_dbt_queries(lookback_days=7, top_n=20, min_total_credits=0.1, credit_rate_usd=2, high_cost_threshold_usd=10000, dbt_project_only=true, include_package_models=false) %}
  {{ return(adapter.dispatch('find_expensive_dbt_queries', 'dbt_cost_optimization_package')(
      lookback_days=lookback_days,
      top_n=top_n,
      min_total_credits=min_total_credits,
      credit_rate_usd=credit_rate_usd,
      high_cost_threshold_usd=high_cost_threshold_usd,
      dbt_project_only=dbt_project_only,
      include_package_models=include_package_models
  )) }}
{% endmacro %}

{% macro default__find_expensive_dbt_queries(lookback_days=7, top_n=20, min_total_credits=0.1, credit_rate_usd=2, high_cost_threshold_usd=10000, dbt_project_only=true, include_package_models=false) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "find_expensive_dbt_queries is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
