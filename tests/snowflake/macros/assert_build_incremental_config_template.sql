{#--
  build_incremental_config_template renders a copy-pasteable dbt config block from
  incremental_strategy, suggested_filter_column, and best_unique_key. Each of its five
  branches must contain its own settings and none of the other branches'.
  Checks key fragments rather than exact text, so formatting changes don't break it.
  Returns rows only on mismatch.
--#}
with inputs as (
    select 'microbatch'               as case_name, 'microbatch'    as incremental_strategy, 'EVENT_TS'   as suggested_filter_column, null as best_unique_key
    union all select 'merge with filter',          'merge',          'UPDATED_AT', 'ID'
    union all select 'delete+insert, no filter',   'delete+insert',  null,         null
    union all select 'append with filter',         'append',         'LOADED_AT',  null
    union all select 'append, no filter',          'append',         null,         null
),

rendered as (
    select case_name, {{ build_incremental_config_template() }} as template
    from inputs
),

checks as (
    select case_name, template,
        case case_name
            when 'microbatch' then
                template like '%incremental_strategy=''microbatch''%'
                and template like '%event_time=''event_ts''%'
                and template like '%batch_size=''day''%'
                and template not like '%unique_key%'
            when 'merge with filter' then
                template like '%incremental_strategy=''merge''%'
                and template like '%unique_key=''id''%'
                and template like '%on_schema_change=''append_new_columns''%'
                and template like {% raw %}'%{% if is_incremental() %}%'{% endraw %}
                and template like {% raw %}'%where updated_at > (select max(updated_at) from {{ this }})%'{% endraw %}
                and template not like '%TODO%'
            when 'delete+insert, no filter' then
                template like '%incremental_strategy=''delete+insert''%'
                and template like '%unique_key=''<unique_key>''%'
                and template like '%TODO: add a filter column%'
                and template not like '%is_incremental%'
            when 'append with filter' then
                template like '%incremental_strategy=''append''%'
                and template like '%where loaded_at > (select max(loaded_at) from%'
                and template not like '%unique_key%'
                and template not like '%TODO%'
            when 'append, no filter' then
                template like '%incremental_strategy=''append''%'
                and template like '%TODO: add a filter column%'
                and template like '%TODO: verify data is truly append-only%'
                and template not like '%is_incremental%'
        end as passed
    from rendered
)

select case_name, template
from checks
where not coalesce(passed, false)
