{#--
  Dispatcher for get_column_usage_count. Platform implementations:
    - snowflake__get_column_usage_count   (macros/platforms/snowflake/optimizations/helpers/snowflake__get_column_usage_count.sql)
  Add a <platform>__get_column_usage_count with the same arguments to support another platform.
--#}
{% macro get_column_usage_count(column_name, model_relation, days_to_check=7) %}
  {{ return(adapter.dispatch('get_column_usage_count', 'dbt_cost_optimization_package')(
      column_name=column_name,
      model_relation=model_relation,
      days_to_check=days_to_check
  )) }}
{% endmacro %}

{% macro default__get_column_usage_count(column_name, model_relation, days_to_check=7) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "get_column_usage_count is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
