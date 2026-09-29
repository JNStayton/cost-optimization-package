{{ config(alias='query_attribution_history') }}
{#- Stand-in for ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY: empty, so the package estimates
    per-query credits from elapsed time (credits_from_attribution = false). -#}
select * from snowflake.account_usage.query_attribution_history where false
