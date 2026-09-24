{#--
  Dispatcher for scope_filter. Platform implementations:
    - snowflake__scope_filter   (macros/snowflake/utils/snowflake__scope_filter.sql)
  Add a <platform>__scope_filter with the same arguments to support another platform.
--#}
{% macro scope_filter(project_col='node_project_name', allow_null_col='node_id') %}
  {{ return(adapter.dispatch('scope_filter', 'dbt_cost_optimization_package')(
      project_col=project_col,
      allow_null_col=allow_null_col
  )) }}
{% endmacro %}

{% macro default__scope_filter(project_col='node_project_name', allow_null_col='node_id') %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "scope_filter is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
