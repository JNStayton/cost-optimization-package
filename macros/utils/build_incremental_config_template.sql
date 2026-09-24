{#--
  Dispatcher for build_incremental_config_template. Platform implementations:
    - snowflake__build_incremental_config_template   (macros/platforms/snowflake/utils/snowflake__build_incremental_config_template.sql)
  Add a <platform>__build_incremental_config_template with the same arguments to support another platform.
--#}
{% macro build_incremental_config_template() %}
  {{ return(adapter.dispatch('build_incremental_config_template', 'dbt_cost_optimization_package')()) }}
{% endmacro %}

{% macro default__build_incremental_config_template() %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "build_incremental_config_template is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
