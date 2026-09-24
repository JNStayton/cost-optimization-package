{#--
  Dispatcher for refresh_warehouse_config. Platform implementations:
    - snowflake__refresh_warehouse_config   (macros/snowflake/utils/snowflake__refresh_warehouse_config.sql)
  Add a <platform>__refresh_warehouse_config with the same arguments to support another platform.
--#}
{% macro refresh_warehouse_config() %}
  {{ return(adapter.dispatch('refresh_warehouse_config', 'dbt_cost_optimization_package')()) }}
{% endmacro %}

{% macro default__refresh_warehouse_config() %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "refresh_warehouse_config is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
