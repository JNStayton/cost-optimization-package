{{ config(alias='warehouse_metering_history') }}
{#- Stand-in for ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY: one hourly row per fixture
    warehouse per day, in the hour its queries run. Types come from the real view. -#}
select warehouse_id, warehouse_name, start_time, end_time, credits_used, credits_used_compute, credits_used_cloud_services
from snowflake.account_usage.warehouse_metering_history where false
{% for w in demo_warehouse_catalog() %}
{%- for d in range(1, 7) %}
union all
select {{ w.id }}, '{{ w.name }}',
       date_trunc('hour', dateadd(day, -{{ d }}, current_timestamp())),
       dateadd(hour, 1, date_trunc('hour', dateadd(day, -{{ d }}, current_timestamp()))),
       {{ w.credits }}, {{ w.compute }}, 0
{%- endfor %}
{%- endfor %}
