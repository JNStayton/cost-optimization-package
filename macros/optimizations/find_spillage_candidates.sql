{#--
  Dispatcher for find_spillage_candidates. Platform implementations:
    - snowflake__find_spillage_candidates   (macros/snowflake/optimizations/find_spillage_candidates.sql)
  Add a <platform>__find_spillage_candidates with the same arguments to support another platform.
--#}
{% macro find_spillage_candidates(lookback_days=7, min_total_gb_spilled=0.05, min_runs=1) %}
  {{ return(adapter.dispatch('find_spillage_candidates', 'dbt_cost_optimization_package')(
      lookback_days=lookback_days,
      min_total_gb_spilled=min_total_gb_spilled,
      min_runs=min_runs
  )) }}
{% endmacro %}

{% macro default__find_spillage_candidates(lookback_days=7, min_total_gb_spilled=0.05, min_runs=1) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "find_spillage_candidates is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
