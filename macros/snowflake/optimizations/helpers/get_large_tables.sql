{% macro get_large_tables(min_size_gb=1, target_databases=[], target_schemas=[]) %}
  {#--
    Returns tables above the min size threshold from ACCOUNT_USAGE.
    Standalone helper — queries Snowflake directly without model dependencies.

    Returns columns: TABLE_DATABASE, TABLE_SCHEMA, TABLE_NAME, SIZE_GB,
                     ROW_COUNT, TABLE_TYPE, APPROX_MICROPARTITIONS
  --#}

  {% set sql %}
    select
        t.table_catalog as table_database,
        t.table_schema,
        t.table_name,
        t.table_type,
        t.row_count,
        s.active_bytes / power(1024, 3) as size_gb,
        s.active_bytes / (16 * 1024 * 1024) as approx_micropartitions
    from snowflake.account_usage.tables as t
    inner join snowflake.account_usage.table_storage_metrics as s
        on t.table_catalog = s.table_catalog
        and t.table_schema = s.table_schema
        and t.table_name = s.table_name
    where t.deleted is null
      and s.deleted = false
      and t.table_type in ('BASE TABLE', 'MATERIALIZED VIEW')
      and s.active_bytes / power(1024, 3) >= {{ min_size_gb }}
    {% if target_databases | length > 0 %}
      and upper(t.table_catalog) in (
          {% for db in target_databases %}'{{ db | upper }}'{% if not loop.last %}, {% endif %}{% endfor %}
      )
    {% endif %}
    {% if target_schemas | length > 0 %}
      and upper(t.table_schema) in (
          {% for sc in target_schemas %}'{{ sc | upper }}'{% if not loop.last %}, {% endif %}{% endfor %}
      )
    {% endif %}
    qualify row_number() over (
        partition by t.table_catalog, t.table_schema, t.table_name
        order by s.id desc
    ) = 1
    order by size_gb desc
  {% endset %}

  {% set results = run_query(sql) %}
  {{ return(results) }}

{% endmacro %}
