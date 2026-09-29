{{ config(alias='metering_history') }}
{#- Stand-in for ACCOUNT_USAGE.METERING_HISTORY: empty, with the real view's columns and types. -#}
select * from snowflake.account_usage.metering_history where false
