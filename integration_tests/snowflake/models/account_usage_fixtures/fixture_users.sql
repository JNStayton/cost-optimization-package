{{ config(alias='users') }}
{#- Stand-in for ACCOUNT_USAGE.USERS: empty, with the real view's columns and types. -#}
select * from snowflake.account_usage.users where false
