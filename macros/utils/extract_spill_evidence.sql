{#--
  Post-hook that samples spilling queries' operator statistics.
  Dispatches to the platform implementation:
    - snowflake__extract_spill_evidence   (macros/platforms/snowflake/utils/snowflake__extract_spill_evidence.sql)
--#}
{% macro extract_spill_evidence() %}
  {{ return(adapter.dispatch('extract_spill_evidence', 'dbt_cost_optimization_package')()) }}
{% endmacro %}

{% macro default__extract_spill_evidence() %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "extract_spill_evidence is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
