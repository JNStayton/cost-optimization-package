{% macro find_spillage_candidates(lookback_days=7, min_total_gb_spilled=0.05, min_runs=1, preview_only=true) %}

  {#--
    Identifies dbt-managed tables whose builds are spilling to local or remote
    storage during the lookback window.

    Remote spillage is the critical signal — it means the compute exhausted RAM
    and fell back to S3, which dramatically increases elapsed time and credit
    consumption. Local spillage is less severe but indicates either oversized
    intermediate results in the query plan or an undersized warehouse.

    Recommendation tiers (encoded as recommendation_key in SQL):
      remote_spill   (warn) - any meaningful remote spillage; refactor or scale up
      local_heavy    (warn) - local spillage > 5 GB total; refactor or scale up
      local_moderate (info) - tracked spillage above the floor but not severe

    Requires Snowflake Enterprise Edition (access_history is Enterprise+).

    How to run:
      dbt run-operation find_spillage_candidates

    With custom args:
      dbt run-operation find_spillage_candidates \
        --args '{lookback_days: 14, min_total_gb_spilled: 5, min_runs: 5}'

    [TODO] Persist results to a model so we can track spillage history over time.
  --#}

  {% if execute %}

    {{ log("--- Starting Spillage Candidate Analysis ---", info=true) }}
    {{ log("Criteria: dbt-managed tables with >= " ~ min_runs ~ " runs and >= " ~ min_total_gb_spilled ~ " GB total spillage in last " ~ lookback_days ~ " days.", info=true) }}

    {% set performance_sql %}
      with dbt_sessions as (
          {{ dbt_cost_optimization_package.dbt_session_filter(lookback_days=lookback_days) }}
      ),

      dbt_writes as (
          select
              q.query_id,
              q.start_time,
              q.warehouse_name,
              q.warehouse_size,
              q.total_elapsed_time,
              q.bytes_spilled_to_local_storage,
              q.bytes_spilled_to_remote_storage,
              t.table_catalog as database_name,
              t.table_schema as schema_name,
              t.table_name as table_name
          from
              snowflake.account_usage.query_history q
          inner join
              dbt_sessions s on q.session_id = s.session_id
          inner join
              snowflake.account_usage.access_history h on q.query_id = h.query_id
          , lateral flatten(input => h.objects_modified) f
          inner join
              snowflake.account_usage.tables t on f.value:objectId::number = t.table_id
          where
              q.start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
              and h.query_start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
              and q.execution_status = 'SUCCESS'
      ),

      spillage_summary as (
          select
              database_name,
              schema_name,
              table_name,
              count(distinct query_id) as total_runs,
              any_value(warehouse_name) as warehouse_name,
              any_value(warehouse_size) as warehouse_size,
              any_value(query_id) as sample_query_id,
              round(avg(total_elapsed_time / 1000), 2) as avg_elapsed_sec,
              round(sum(bytes_spilled_to_local_storage) / power(1024, 3), 2) as total_gb_spilled_local,
              round(sum(bytes_spilled_to_remote_storage) / power(1024, 3), 2) as total_gb_spilled_remote,
              round(avg(bytes_spilled_to_local_storage) / power(1024, 3), 2) as avg_gb_spilled_local,
              round(avg(bytes_spilled_to_remote_storage) / power(1024, 3), 2) as avg_gb_spilled_remote
          from
              dbt_writes
          group by 1, 2, 3
          having
              count(distinct query_id) >= {{ min_runs }}
              and (total_gb_spilled_local + total_gb_spilled_remote) >= {{ min_total_gb_spilled }}
      )

      {#-
        recommendation_key encodes the tier logic in SQL — string equality
        compares reliably across types in minijinja.

        tier thresholds (in GB):
          remote_spill:  any total_gb_spilled_remote > 0.1
          local_heavy:   total_gb_spilled_local > 5 (and no remote)
          local_moderate: anything else above the min_total_gb_spilled floor
      -#}
      select
          database_name,
          schema_name,
          table_name,
          total_runs,
          warehouse_name,
          warehouse_size,
          sample_query_id,
          avg_elapsed_sec,
          total_gb_spilled_local,
          total_gb_spilled_remote,
          avg_gb_spilled_local,
          avg_gb_spilled_remote,
          case
              when total_gb_spilled_remote > 0.1 then 'remote_spill'
              when total_gb_spilled_local > 5 then 'local_heavy'
              else 'local_moderate'
          end as recommendation_key
      from
          spillage_summary
      order by
          total_gb_spilled_remote desc,
          total_gb_spilled_local desc
    {% endset %}

    {% set results = run_query(performance_sql) %}

    {% if results.rows | length == 0 %}
        {{ log("No spillage candidates met the criteria. Try lowering min_total_gb_spilled or min_runs, or extending lookback_days.", info=true) }}
    {% else %}

        {{ log("Found " ~ results.rows | length ~ " candidate(s).", info=true) }}
        {{ log("", info=true) }}

        {% set recommendations = [] %}

        {% for row in results.rows %}
            {% set database_name = row["DATABASE_NAME"] %}
            {% set schema_name = row["SCHEMA_NAME"] %}
            {% set table_name = row["TABLE_NAME"] %}
            {% set total_runs = row["TOTAL_RUNS"] %}
            {% set warehouse_name = row["WAREHOUSE_NAME"] %}
            {% set warehouse_size = row["WAREHOUSE_SIZE"] %}
            {% set sample_query_id = row["SAMPLE_QUERY_ID"] %}
            {% set avg_elapsed_sec = row["AVG_ELAPSED_SEC"] %}
            {% set total_gb_spilled_local = row["TOTAL_GB_SPILLED_LOCAL"] %}
            {% set total_gb_spilled_remote = row["TOTAL_GB_SPILLED_REMOTE"] %}
            {% set avg_gb_spilled_local = row["AVG_GB_SPILLED_LOCAL"] %}
            {% set avg_gb_spilled_remote = row["AVG_GB_SPILLED_REMOTE"] %}
            {% set recommendation_key = row["RECOMMENDATION_KEY"] %}

            {% set fqn_key = database_name ~ "." ~ schema_name ~ "." ~ table_name %}

            {# attach dbt graph node so we can surface current materialization #}
            {% set ns = namespace(model_node=none) %}
            {{ log("DEBUG target fqn: " ~ fqn_key, info=true) }}
            {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
                {% set node_identifier = node.alias if node.alias else node.name %}
                {% set node_relation = node.relation_name | replace('"', '') if node.relation_name else 'NONE' %}

                {% if node_identifier | upper in ['FCT_ORDER_ITEMS', 'INT_ORDER_ITEMS', 'INT_CUSTOMER_ORDERS'] %}
                    {{ log(
                        "DEBUG node => unique_id=" ~ node.unique_id
                        ~ " | database=" ~ (node.database or 'NONE')
                        ~ " | schema=" ~ (node.schema or 'NONE')
                        ~ " | alias=" ~ (node.alias or 'NONE')
                        ~ " | name=" ~ (node.name or 'NONE')
                        ~ " | identifier=" ~ node_identifier
                        ~ " | relation_name=" ~ node_relation,
                        info=true
                    ) }}
                {% endif %}

                {% if node.database
                      and node.schema
                      and node_identifier
                      and node.database | upper == database_name | upper
                      and node.schema | upper == schema_name | upper
                      and node_identifier | upper == table_name | upper %}
                    {{ log("DEBUG matched node: " ~ node.unique_id, info=true) }}
                    {% set ns.model_node = node %}
                    {% break %}
                {% endif %}
            {% endfor %}

            {% set current_materialization = ns.model_node.config.materialized if ns.model_node else 'N/A (not in dbt project)' %}
            {% set recommendation = none %}
            {% set reason = none %}
            {% set severity = 'info' %}

            {% if recommendation_key == 'remote_spill' %}
                {% set recommendation = 'Critical: refactor SQL or scale up warehouse (remote spillage)' %}
                {% set reason = total_gb_spilled_remote ~ ' GB of remote spillage — compute exhausted RAM and fell back to S3. Major elapsed-time and cost impact.' %}
                {% set severity = 'warn' %}
            {% elif recommendation_key == 'local_heavy' %}
                {% set recommendation = 'Refactor SQL or scale up warehouse (significant local spill)' %}
                {% set reason = total_gb_spilled_local ~ ' GB of local spillage — intermediate results exceeded RAM. SQL-side fixes are usually cheaper than scaling up.' %}
                {% set severity = 'warn' %}
            {% else %}
                {% set recommendation = 'Monitor or consider SQL refactor (moderate local spill)' %}
                {% set reason = total_gb_spilled_local ~ ' GB of local spillage observed across ' ~ total_runs ~ ' runs. Tolerable but worth profiling if this model is on a critical path.' %}
            {% endif %}

            {% do recommendations.append({
                'fqn': fqn_key,
                'current_materialization': current_materialization,
                'total_runs': total_runs,
                'warehouse_name': warehouse_name,
                'warehouse_size': warehouse_size,
                'sample_query_id': sample_query_id,
                'avg_elapsed_sec': avg_elapsed_sec,
                'total_gb_spilled_local': total_gb_spilled_local,
                'total_gb_spilled_remote': total_gb_spilled_remote,
                'avg_gb_spilled_local': avg_gb_spilled_local,
                'avg_gb_spilled_remote': avg_gb_spilled_remote,
                'recommendation': recommendation,
                'reason': reason,
                'severity': severity
            }) %}
        {% endfor %}

        {# warns first; SQL already sorted by remote desc then local desc, preserving that order within buckets #}
        {% set warns = recommendations | selectattr('severity', 'equalto', 'warn') | list %}
        {% set infos = recommendations | selectattr('severity', 'equalto', 'info') | list %}
        {% set sorted_recs = warns + infos %}

        {{ log("--- Recommendations ---", info=true) }}

        {% for r in sorted_recs %}
            {% if preview_only and loop.index > 10 %}{% break %}{% endif %}

            {{ dbt_cost_optimization_package.log_recommendation(
                title='Model: ' ~ r.fqn,
                recommendation=r.recommendation,
                reason=r.reason,
                severity=r.severity,
                metrics={
                    'current_materialization': r.current_materialization,
                    'total_runs': r.total_runs,
                    'warehouse': r.warehouse_name ~ ' (' ~ r.warehouse_size ~ ')',
                    'avg_elapsed_sec': r.avg_elapsed_sec,
                    'total_gb_spilled_local': r.total_gb_spilled_local,
                    'total_gb_spilled_remote': r.total_gb_spilled_remote,
                    'avg_gb_spilled_local': r.avg_gb_spilled_local,
                    'avg_gb_spilled_remote': r.avg_gb_spilled_remote,
                    'sample_query_id': r.sample_query_id
                }
            ) }}
        {% endfor %}

    {% endif %}

  {% endif %}

{% endmacro %}
