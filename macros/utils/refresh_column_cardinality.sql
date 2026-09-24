{#--
  Dispatcher for refresh_column_cardinality. Platform implementations:
    - snowflake__refresh_column_cardinality   (macros/snowflake/utils/refresh_column_cardinality.sql)
  Add a <platform>__refresh_column_cardinality with the same arguments to support another platform.
--#}
{% macro refresh_column_cardinality() %}
  {{ return(adapter.dispatch('refresh_column_cardinality', 'dbt_cost_optimization_package')()) }}
{% endmacro %}

{% macro default__refresh_column_cardinality() %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "refresh_column_cardinality is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
