{#--
  Dispatcher for extract_operator_evidence. Platform implementations:
    - snowflake__extract_operator_evidence   (macros/platforms/snowflake/utils/snowflake__extract_operator_evidence.sql)
  Add a <platform>__extract_operator_evidence with the same arguments to support another platform.
--#}
{% macro extract_operator_evidence() %}
  {{ return(adapter.dispatch('extract_operator_evidence', 'dbt_cost_optimization_package')()) }}
{% endmacro %}

{% macro default__extract_operator_evidence() %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "extract_operator_evidence is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
