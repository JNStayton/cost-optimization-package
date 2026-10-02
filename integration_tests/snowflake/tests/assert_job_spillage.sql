{#-
  Phase S3: job-level spillage (both editions). Four dbt platform jobs in the fixture
  query history, 2 runs each:
    - 7001 (BUSY, Medium): 2 of 10 models spill and take 91% of build time → route those
      two models (actionable) to a warehouse one size up (Large). The config is a
      placeholder ('<larger warehouse>'): the package doesn't pick a warehouse.
    - 7002 (JOBS, Medium, no other job): 4 of 8 spill (50%), 91% → size up the job
      (actionable), with DDL to Large. 1,760 build s: 2.49 hours a year saved, +$7.75.
    - 7003 (BUSY): 2 of 10 spill, 45% → route (monitor).
    - 7004 (BUSY): 1 of 10, 7% → nothing.
  Each job gets at most one signal; routed models appear in the dbt model view with the
  snowflake_warehouse config, and the job size-up in the warehouse view.
  Returns rows only on failure.
-#}
with jobs as (
    select job_key, signal_id, backlog_status, spilling_model_share_pct, spilling_time_share_pct,
           warehouse_shared, next_warehouse_size, evidence
    from {{ ref('int_snowflake__dbt_job_spillage') }}
),

recs as (
    select signal_id, entity_name, backlog_status, snowflake_ddl, dbt_model_config,
           estimated_annual_savings_usd, estimated_annual_hours_saved, estimated_annual_cost_change_usd
    from {{ ref('int_snowflake__all_recommendations') }}
    where signal_id in ('spillage_route_models', 'spillage_job_scale_up')
),

checks as (
    select 'job signals and statuses' as check_name,
        (select listagg(job_key || ':' || coalesce(signal_id, 'none') || ':' || coalesce(backlog_status, 'none'), ',')
                within group (order by job_key) from jobs) as produced,
        '7001:spillage_route_models:actionable,7002:spillage_job_scale_up:actionable,'
            || '7003:spillage_route_models:monitor,7004:none:none' as expected
    union all select 'shares (model %, time %)',
        (select listagg(job_key || ':' || spilling_model_share_pct::number(5, 1) || '/' || spilling_time_share_pct::number(5, 1), ',')
                within group (order by job_key) from jobs),
        '7001:20.0/90.9,7002:50.0/90.9,7003:20.0/45.5,7004:10.0/6.9'
    union all select 'evidence text',
        (select evidence from jobs where job_key = '7001'),
        '2 of 10 models (20%) took 91% of this job''s build time (2 run(s) in 30 days).'
    union all select 'shared warehouse',
        (select listagg(job_key || ':' || warehouse_shared, ',') within group (order by job_key) from jobs),
        '7001:true,7002:false,7003:true,7004:true'
    union all select 'next size up for routing',
        (select next_warehouse_size from jobs where job_key = '7001'),
        'LARGE'
    union all select 'one signal per job (routing or size-up, never both)',
        (select count(*) from (
            select coalesce(js.job_key, 'none') as job_key, count(distinct r.signal_id) as n
            from recs as r
            left join jobs as js
                on r.entity_name = 'dbt job ' || js.job_key
                or contains(r.entity_name, 'job' || js.job_key || '_')
            group by 1 having count(distinct r.signal_id) > 1))::varchar,
        '0'
    union all select 'routed models in the dbt model view, with the config',
        (select listagg(model_name || ':' || dbt_model_config, ',') within group (order by model_name)
         from {{ ref('vw_snowflake__dbt_model_optimizations') }} where signal_id = 'spillage_route_models'),
        'job7001_m0:{{ "{{" }} config(snowflake_warehouse=''<larger warehouse>'') {{ "}}" }},'
            || 'job7001_m1:{{ "{{" }} config(snowflake_warehouse=''<larger warehouse>'') {{ "}}" }}'
    union all select 'job size-up in the warehouse view, with DDL',
        (select listagg(warehouse_name || ':' || recommendation || ':' || snowflake_ddl, ',')
         from {{ ref('vw_snowflake__warehouse_optimizations') }} where signal_id = 'spillage_job_scale_up'),
        'FIXTURE_WH_JOBS:Scale up this job''s warehouse:ALTER WAREHOUSE FIXTURE_WH_JOBS SET WAREHOUSE_SIZE = ''LARGE'';'
    union all select 'time vs cost (savings null)',
        (select listagg(split_part(entity_name, '.', -1) || ':' || coalesce(estimated_annual_savings_usd::varchar, 'null')
                        || ':' || estimated_annual_hours_saved::number(10, 2) || ':' || estimated_annual_cost_change_usd::number(10, 2), ',')
                within group (order by entity_name) from recs),
        'dbt job 7002:null:2.49:7.75,job7001_m0:null:1.13:3.52,job7001_m1:null:1.13:3.52,'
            || 'job7003_m0:null:0.28:0.88,job7003_m1:null:0.28:0.88'
)

select * from checks where produced is distinct from expected
