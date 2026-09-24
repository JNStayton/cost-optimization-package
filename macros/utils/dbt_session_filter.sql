{#--
  Dispatcher for dbt_session_filter. Platform implementations:
    - snowflake__dbt_session_filter   (macros/snowflake/utils/dbt_session_filter.sql)
  Add a <platform>__dbt_session_filter with the same arguments to support another platform.
--#}
{% macro dbt_session_filter(lookback_days=7) %}
  {{ return(adapter.dispatch('dbt_session_filter', 'dbt_cost_optimization_package')(
      lookback_days=lookback_days
  )) }}
{% endmacro %}

{% macro default__dbt_session_filter(lookback_days=7) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "dbt_session_filter is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
