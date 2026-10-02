{#-
  The fixture warehouses for the warehouse slice. Each one is shaped to land in one branch
  of fct_snowflake__warehouse_config_recommendations (Standard edition path). The
  query_history, sessions, warehouse_metering_history and warehouse_events_history
  fixtures all read this list.

  Per warehouse, per day for 6 days: 4 queries from its own dbt session, plus one metering
  row (credits / compute credits). Query times are in ms; load is QUERY_LOAD_PERCENT.
  event_size uses WAREHOUSE_EVENTS_HISTORY's format (XSMALL); qh_size uses
  QUERY_HISTORY's (X-Small). node_id, when set, goes in a dbt query comment.
  JOBS is for the job-level spillage slice: a Medium warehouse that runs one dbt platform
  job alone, shaped like HEALTHY so it gets no config recommendation of its own.
  BURSTY and the three MCW_ warehouses are for the multi-cluster branches (Enterprise).
  Their SHOW WAREHOUSES settings (auto-suspend, scaling policy, cluster counts) come from
  macros/simulate_show_warehouses.sql; on Standard they keep the defaults.
-#}
{% macro demo_warehouse_catalog() %}
  {{ return([
    {'name': 'FIXTURE_WH_IDLE',       'id': 800001, 'session_id': 101, 'event_size': 'SMALL',  'qh_size': 'Small',
     'elapsed': 2000, 'exec': 2000, 'overload': 0, 'provisioning': 0, 'load': 60,
     'credits': 1.0, 'compute': 0.5, 'autosuspends': 30, 'suspended_last': false, 'node_id': none},
    {'name': 'FIXTURE_WH_BUSY',       'id': 800002, 'session_id': 102, 'event_size': 'MEDIUM', 'qh_size': 'Medium',
     'elapsed': 5000, 'exec': 3000, 'overload': 2000, 'provisioning': 0, 'load': 100,
     'credits': 10.0, 'compute': 9.5, 'autosuspends': 0, 'suspended_last': false,
     'node_id': 'model.cost_optimization_integration_tests.demo_orders'},
    {'name': 'FIXTURE_WH_BUSY_XS',    'id': 800003, 'session_id': 103, 'event_size': 'XSMALL', 'qh_size': 'X-Small',
     'elapsed': 5000, 'exec': 3000, 'overload': 2000, 'provisioning': 0, 'load': 100,
     'credits': 1.0, 'compute': 0.95, 'autosuspends': 0, 'suspended_last': false, 'node_id': none},
    {'name': 'FIXTURE_WH_BUSY_2XL',   'id': 800008, 'session_id': 108, 'event_size': 'XXLARGE', 'qh_size': '2X-Large',
     'elapsed': 5000, 'exec': 3000, 'overload': 2000, 'provisioning': 0, 'load': 100,
     'credits': 1.0, 'compute': 0.95, 'autosuspends': 0, 'suspended_last': false, 'node_id': none},
    {'name': 'FIXTURE_WH_BUSY_6XL',   'id': 800009, 'session_id': 109, 'event_size': 'X6LARGE', 'qh_size': '6X-Large',
     'elapsed': 5000, 'exec': 3000, 'overload': 2000, 'provisioning': 0, 'load': 100,
     'credits': 1.0, 'compute': 0.95, 'autosuspends': 0, 'suspended_last': false, 'node_id': none},
    {'name': 'FIXTURE_WH_COLD',       'id': 800004, 'session_id': 104, 'event_size': 'SMALL',  'qh_size': 'Small',
     'elapsed': 8000, 'exec': 3000, 'overload': 0, 'provisioning': 5000, 'load': 60,
     'credits': 1.0, 'compute': 0.95, 'autosuspends': 0, 'suspended_last': false, 'node_id': none},
    {'name': 'FIXTURE_WH_OVERSIZED',  'id': 800005, 'session_id': 105, 'event_size': 'LARGE',  'qh_size': 'Large',
     'elapsed': 100, 'exec': 100, 'overload': 0, 'provisioning': 0, 'load': 10,
     'credits': 1.0, 'compute': 0.95, 'autosuspends': 0, 'suspended_last': false,
     'node_id': 'model.other_project.big_model'},
    {'name': 'FIXTURE_WH_HEALTHY',    'id': 800006, 'session_id': 106, 'event_size': 'SMALL',  'qh_size': 'Small',
     'elapsed': 2000, 'exec': 2000, 'overload': 0, 'provisioning': 0, 'load': 60,
     'credits': 1.0, 'compute': 0.95, 'autosuspends': 0, 'suspended_last': false,
     'node_id': 'model.cost_optimization_integration_tests.demo_logs'},
    {'name': 'FIXTURE_WH_JOBS',       'id': 800010, 'session_id': 111, 'event_size': 'MEDIUM', 'qh_size': 'Medium',
     'elapsed': 2000, 'exec': 2000, 'overload': 0, 'provisioning': 0, 'load': 60,
     'credits': 1.0, 'compute': 0.95, 'autosuspends': 0, 'suspended_last': false, 'node_id': none},
    {'name': 'FIXTURE_WH_BURSTY',     'id': 800011, 'session_id': 112, 'event_size': 'SMALL',  'qh_size': 'Small',
     'elapsed': 2000, 'exec': 2000, 'overload': 0, 'provisioning': 0, 'load': 90,
     'credits': 1.0, 'compute': 0.5, 'autosuspends': 0, 'suspended_last': false, 'node_id': none},
    {'name': 'FIXTURE_WH_MCW_IDLE',   'id': 800012, 'session_id': 113, 'event_size': 'SMALL',  'qh_size': 'Small',
     'elapsed': 2000, 'exec': 2000, 'overload': 0, 'provisioning': 0, 'load': 60,
     'credits': 1.0, 'compute': 0.5, 'autosuspends': 0, 'suspended_last': false, 'node_id': none},
    {'name': 'FIXTURE_WH_MCW_BUSY',   'id': 800013, 'session_id': 114, 'event_size': 'MEDIUM', 'qh_size': 'Medium',
     'elapsed': 5000, 'exec': 3000, 'overload': 2000, 'provisioning': 0, 'load': 100,
     'credits': 10.0, 'compute': 9.5, 'autosuspends': 0, 'suspended_last': false, 'node_id': none},
    {'name': 'FIXTURE_WH_MCW_OVERSIZED', 'id': 800014, 'session_id': 115, 'event_size': 'LARGE', 'qh_size': 'Large',
     'elapsed': 100, 'exec': 100, 'overload': 0, 'provisioning': 0, 'load': 10,
     'credits': 1.0, 'compute': 0.95, 'autosuspends': 0, 'suspended_last': false, 'node_id': none},
    {'name': 'FIXTURE_WH_SUSPENDED',  'id': 800007, 'session_id': 107, 'event_size': 'XSMALL', 'qh_size': 'X-Small',
     'elapsed': 100, 'exec': 100, 'overload': 0, 'provisioning': 0, 'load': 10,
     'credits': 1.0, 'compute': 0.95, 'autosuspends': 0, 'suspended_last': true, 'node_id': none},
  ]) }}
{% endmacro %}
