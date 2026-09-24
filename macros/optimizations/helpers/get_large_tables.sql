{#--
  Dispatcher for get_large_tables. Platform implementations:
    - snowflake__get_large_tables   (macros/snowflake/optimizations/helpers/get_large_tables.sql)
  Add a <platform>__get_large_tables with the same arguments to support another platform.
--#}
{% macro get_large_tables(min_size_gb=1, target_databases=[], target_schemas=[]) %}
  {{ return(adapter.dispatch('get_large_tables', 'dbt_cost_optimization_package')(
      min_size_gb=min_size_gb,
      target_databases=target_databases,
      target_schemas=target_schemas
  )) }}
{% endmacro %}

{% macro default__get_large_tables(min_size_gb=1, target_databases=[], target_schemas=[]) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "get_large_tables is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
