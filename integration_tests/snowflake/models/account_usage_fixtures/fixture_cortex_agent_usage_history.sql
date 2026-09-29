{{ config(alias='cortex_agent_usage_history') }}
{#- Stand-in for ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY: empty, with the real view's columns and types. -#}
select * from snowflake.account_usage.cortex_agent_usage_history where false
