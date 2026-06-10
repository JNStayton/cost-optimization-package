{% macro dbt_session_filter(lookback_days=7) %}

  {#--
    Returns a SELECT statement that finds session_ids for sessions authenticated by dbt,
    filtered to the given lookback window. Intended to be wrapped in a CTE by the caller.

    This is the shared scoping CTE used by the warehouse/spillage/queue recommendation
    macros so that all dbt-scoped optimization queries agree on what "a dbt session" means.

    Usage:

      with dbt_sessions as (
          {{ dbt_cost_optimization_package.dbt_session_filter(lookback_days=7) }}
      ),
      ...

    Future: when query tags are widely adopted in the project, swap the
    client_environment check for a query_tag filter for better accuracy and
    performance. That change should only need to happen here.
  --#}

  select distinct
      session_id
  from
      snowflake.account_usage.sessions
  where
      created_on >= dateadd(day, -{{ lookback_days }}, current_timestamp())
      and parse_json(client_environment):APPLICATION::string = 'dbt'

{% endmacro %}
