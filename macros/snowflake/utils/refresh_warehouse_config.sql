{% macro refresh_warehouse_config() %}

  {#--
    Captures current warehouse configuration from SHOW WAREHOUSES and merges
    into int_snowflake__warehouse_config. Provides settings not available in
    any ACCOUNT_USAGE view: auto_suspend, auto_resume, scaling_policy,
    min_cluster_count, max_cluster_count.

    Runs as a post-hook on int_snowflake__warehouse_config so the base model
    builds from WAREHOUSE_EVENTS_HISTORY first, then this macro enriches with
    live config from SHOW WAREHOUSES.

    SHOW WAREHOUSES returns one row per warehouse — no looping needed.
  --#}

  {% if execute and target.type == 'snowflake' %}

    {% set config_table = ref('int_snowflake__warehouse_config') %}

    {{ log("refresh_warehouse_config: running SHOW WAREHOUSES...", info=true) }}

    {# Step 1: Run SHOW WAREHOUSES and capture results into a temp table #}
    {% set show_sql %}
      create or replace temporary table {{ config_table.database }}.{{ config_table.schema }}.tmp_show_warehouses as
      select * from table(result_scan(last_query_id()))
    {% endset %}

    {% do run_query("SHOW WAREHOUSES") %}
    {% do run_query(show_sql) %}

    {# Step 2: Merge SHOW data into the warehouse config table #}
    {% set merge_sql %}
      merge into {{ config_table }} as target
      using (
          select
              "name" as warehouse_name,
              "auto_suspend"::int as auto_suspend_seconds,
              "auto_resume"::boolean as auto_resume,
              coalesce("scaling_policy", 'STANDARD') as scaling_policy,
              coalesce("min_cluster_count", 1)::int as min_cluster_count,
              coalesce("max_cluster_count", 1)::int as max_cluster_count
          from {{ config_table.database }}.{{ config_table.schema }}.tmp_show_warehouses
      ) as source
      on target.warehouse_name = source.warehouse_name
      when matched then update set
          auto_suspend_seconds = source.auto_suspend_seconds,
          auto_resume          = source.auto_resume,
          scaling_policy       = source.scaling_policy,
          min_cluster_count    = source.min_cluster_count,
          max_cluster_count    = source.max_cluster_count
      when not matched then insert
          (warehouse_name, auto_suspend_seconds, auto_resume, scaling_policy, min_cluster_count, max_cluster_count)
      values
          (source.warehouse_name, source.auto_suspend_seconds, source.auto_resume, source.scaling_policy, source.min_cluster_count, source.max_cluster_count)
    {% endset %}

    {% do run_query(merge_sql) %}

    {# Step 3: Clean up temp table #}
    {% do run_query("DROP TABLE IF EXISTS " ~ config_table.database ~ "." ~ config_table.schema ~ ".tmp_show_warehouses") %}

    {{ log("refresh_warehouse_config: merged config for all warehouses.", info=true) }}

  {% endif %}

{% endmacro %}
