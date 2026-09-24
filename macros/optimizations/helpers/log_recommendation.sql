{#--
  Dispatcher for log_recommendation. Platform implementations:
    - snowflake__log_recommendation   (macros/platforms/snowflake/optimizations/helpers/snowflake__log_recommendation.sql)
  Add a <platform>__log_recommendation with the same arguments to support another platform.
--#}
{% macro log_recommendation(title, recommendation, reason, metrics={}, severity='info') %}
  {{ return(adapter.dispatch('log_recommendation', 'dbt_cost_optimization_package')(
      title=title,
      recommendation=recommendation,
      reason=reason,
      metrics=metrics,
      severity=severity
  )) }}
{% endmacro %}

{% macro default__log_recommendation(title, recommendation, reason, metrics={}, severity='info') %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "log_recommendation is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake."
    ) }}
  {% endif %}
{% endmacro %}
