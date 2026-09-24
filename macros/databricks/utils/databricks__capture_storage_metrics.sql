{% macro databricks__capture_storage_metrics() %}

  {#--
    Pre-hook for int_databricks__table_storage.

    system.storage.table_metrics_history can be empty account-wide even when
    Predictive Optimization is actively running elsewhere in the account —
    verified by finding real PO activity in
    system.storage.predictive_optimization_operations_history with zero
    table_metrics_history rows. This is not a Predictive-Optimization-enablement
    gap. ANALYZE TABLE ... COMPUTE STORAGE METRICS is a GA, self-serve command
    with no PO/plan prerequisite that returns the same active_bytes/file-count
    data directly, so it's used here as a fallback when the system table is
    empty.

    Scope: only tables tracked by int_dbt__relations (this dbt project's own
    models) are probed, matching the default dbt_project_only scoping already
    used by fct_databricks__optimize_candidates and
    fct_databricks__liquid_clustering_candidates. int_dbt__relations and
    int_databricks__tables are both independent of int_databricks__table_storage
    and int_databricks__table_inventory, so referencing them here does not
    create a circular dependency.

    The probe table is always created (possibly empty) so
    int_databricks__table_storage can safely select from it regardless of
    whether table_metrics_history already had data.

    Variables:
      storage_metrics_probe_max_tables (default 200)
  --#}

  {% if execute and target.type == 'databricks' %}

    {% set probe_relation = this.database ~ '.' ~ this.schema ~ '.int_databricks__table_storage_metrics_probe' %}

    {% set create_sql %}
      create table if not exists {{ probe_relation }} (
        database_name string,
        schema_name string,
        table_name string,
        active_bytes bigint,
        file_count bigint,
        time_travel_bytes bigint,
        vacuumable_bytes bigint,
        captured_at timestamp
      )
    {% endset %}
    {% do run_query(create_sql) %}

    {% set existing_sql %}
      select count(*) as n from system.storage.table_metrics_history
    {% endset %}
    {% set existing = run_query(existing_sql) %}
    {% set existing_count = (existing.rows[0][0] | int) if existing and existing.rows | length > 0 else 0 %}

    {% if existing_count > 0 %}

      {{ log("capture_storage_metrics: system.storage.table_metrics_history has " ~ existing_count ~ " rows — skipping ANALYZE TABLE fallback probe.", info=true) }}

    {% else %}

      {% set max_tables = var('storage_metrics_probe_max_tables', 200) %}

      {% set candidates_sql %}
        select distinct
            t.database_name,
            t.schema_name,
            t.table_name
        from {{ ref('int_dbt__relations') }} as r
        inner join {{ ref('int_databricks__tables') }} as t
            on lower(r.database_name) = lower(t.database_name)
            and lower(r.schema_name) = lower(t.schema_name)
            and lower(r.table_name) = lower(t.table_name)
        where t.table_type in ('BASE TABLE', 'EXTERNAL TABLE')
            and not t.is_deleted
        limit {{ max_tables }}
      {% endset %}

      {% set candidates = run_query(candidates_sql) %}

      {% if candidates and candidates.rows | length > 0 %}

        {{ log("capture_storage_metrics: table_metrics_history is empty — probing " ~ (candidates.rows | length) ~ " dbt-tracked table(s) via ANALYZE TABLE COMPUTE STORAGE METRICS.", info=true) }}

        {% do run_query("delete from " ~ probe_relation) %}

        {% for row in candidates %}

          {% set db  = row['database_name'] %}
          {% set sch = row['schema_name'] %}
          {% set tbl = row['table_name'] %}
          {% set fqn = db ~ '.' ~ sch ~ '.' ~ tbl %}

          {% set metrics_sql %}
            analyze table {{ fqn }} compute storage metrics
          {% endset %}
          {% set metrics_result = run_query(metrics_sql) %}

          {% set ns = namespace(active_bytes=0, file_count=0, time_travel_bytes=0, vacuumable_bytes=0) %}
          {% for m in metrics_result %}
            {% if m['metric_name'] == 'active_bytes' %}{% set ns.active_bytes = m['metric_value'] %}{% endif %}
            {% if m['metric_name'] == 'num_active_files' %}{% set ns.file_count = m['metric_value'] %}{% endif %}
            {% if m['metric_name'] == 'time_travel_bytes' %}{% set ns.time_travel_bytes = m['metric_value'] %}{% endif %}
            {% if m['metric_name'] == 'vacuumable_bytes' %}{% set ns.vacuumable_bytes = m['metric_value'] %}{% endif %}
          {% endfor %}

          {% set insert_sql %}
            insert into {{ probe_relation }}
            values (
              '{{ db }}', '{{ sch }}', '{{ tbl }}',
              {{ ns.active_bytes }}, {{ ns.file_count }},
              {{ ns.time_travel_bytes }}, {{ ns.vacuumable_bytes }},
              current_timestamp()
            )
          {% endset %}
          {% do run_query(insert_sql) %}

        {% endfor %}

        {{ log("capture_storage_metrics: probe complete.", info=true) }}

      {% else %}
        {{ log("capture_storage_metrics: no dbt-tracked tables found to probe.", info=true) }}
      {% endif %}

    {% endif %}

  {% endif %}

{% endmacro %}
