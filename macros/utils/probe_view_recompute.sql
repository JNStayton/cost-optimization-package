{#--
  Post-hook that measures how long views in view chains take to recompute.
  Dispatches to the platform implementation:
    - snowflake__probe_view_recompute   (macros/platforms/snowflake/utils/snowflake__probe_view_recompute.sql)
--#}
{% macro probe_view_recompute() %}
  {{ return(adapter.dispatch('probe_view_recompute', 'dbt_cost_optimization_package')()) }}
{% endmacro %}

{% macro default__probe_view_recompute() %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "probe_view_recompute is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
