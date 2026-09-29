{{ config(alias='sessions') }}
{#- Stand-in for ACCOUNT_USAGE.SESSIONS: one dbt session per fixture warehouse
    (macros/demo_warehouse_catalog.sql). Types come from the real view. Session 1, used by
    the other slices' queries, is deliberately absent, so those aren't dbt queries. -#}
select session_id, created_on, client_environment
from snowflake.account_usage.sessions where false
{% for w in demo_warehouse_catalog() %}
union all
select {{ w.session_id }}, dateadd(day, -7, current_timestamp()), '{"APPLICATION": "dbt", "OS": "Linux", "VERSION": "2.0.6"}'
{%- endfor %}
