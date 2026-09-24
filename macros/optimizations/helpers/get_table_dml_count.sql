{#--
  Dispatcher for get_table_dml_count. Platform implementations:
    - snowflake__get_table_dml_count   (macros/snowflake/optimizations/helpers/snowflake__get_table_dml_count.sql)
  Add a <platform>__get_table_dml_count with the same arguments to support another platform.
--#}
{% macro get_table_dml_count(database_name, schema_name, table_name, lookback_days=7) %}
  {{ return(adapter.dispatch('get_table_dml_count', 'dbt_cost_optimization_package')(
      database_name=database_name,
      schema_name=schema_name,
      table_name=table_name,
      lookback_days=lookback_days
  )) }}
{% endmacro %}

{% macro default__get_table_dml_count(database_name, schema_name, table_name, lookback_days=7) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "get_table_dml_count is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
