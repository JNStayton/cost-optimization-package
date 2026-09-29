{{ config(alias='cortex_ai_functions_usage_history') }}
{#- Stand-in for ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY: empty, with the real view's columns and types. -#}
select * from snowflake.account_usage.cortex_ai_functions_usage_history where false
