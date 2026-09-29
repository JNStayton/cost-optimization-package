{{ config(alias='warehouse_events_history') }}
{#- Stand-in for ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY (macros/demo_warehouse_catalog.sql).
    As in real data, only WAREHOUSE_CONSISTENT events carry size, cluster count, type and
    resource constraint; suspend events leave them null. Each warehouse has one
    WAREHOUSE_CONSISTENT event an hour ago, except FIXTURE_WH_SUSPENDED, whose latest event
    is an auto-suspend (its WAREHOUSE_CONSISTENT event is two days old). FIXTURE_WH_IDLE
    also has 30 auto-suspend cycles in the last 30 days. Types come from the real view. -#}
select timestamp, warehouse_id, warehouse_name, cluster_number, event_name, event_reason, event_state,
       size, cluster_count, warehouse_type, resource_constraint
from snowflake.account_usage.warehouse_events_history where false
{% for w in demo_warehouse_catalog() %}
union all
select dateadd(hour, {{ -48 if w.suspended_last else -1 }}, current_timestamp()), {{ w.id }}, '{{ w.name }}', null,
       'WAREHOUSE_CONSISTENT', null, null, '{{ w.event_size }}', 1, 'STANDARD', 'STANDARD_GEN_1'
{%- if w.suspended_last %}
union all
select dateadd(minute, -30, current_timestamp()), {{ w.id }}, '{{ w.name }}', 1,
       'SUSPEND_WAREHOUSE', 'WAREHOUSE_AUTOSUSPEND', 'COMPLETED', null, null, null, null
{%- endif %}
{%- for n in range(w.autosuspends) %}
union all
select dateadd(hour, -{{ 3 + n * 23 }}, current_timestamp()), {{ w.id }}, '{{ w.name }}', 1,
       'SUSPEND_WAREHOUSE', 'WAREHOUSE_AUTOSUSPEND', 'COMPLETED', null, null, null, null
{%- endfor %}
{%- endfor %}
