{% macro find_expensive_dbt_queries(
    lookback_days=7,
    top_n=20,
    min_total_credits=0.1,
    credit_rate_usd=2,
    high_cost_threshold_usd=10000,
    dbt_project_only=true
) %}

  {#--
    Surfaces the top-N most expensive dbt-authored queries by projected annual
    cost, grouped by query_hash so repeated runs of the same logical query are
    aggregated together.

    Recommendation tiers (encoded as recommendation_key in SQL):
      high_cost (warn) - projected annual cost exceeds high_cost_threshold_usd
      tracked   (info) - above the min_total_credits floor but below the high bar

    Annual cost is projected from the lookback-window credit consumption,
    multiplied by the credit_rate_usd parameter. Tune credit_rate_usd to your
    contract rate (the source dashboard query hardcoded $2 per credit).

    How to run:
      dbt run-operation find_expensive_dbt_queries

    With custom args:
      dbt run-operation find_expensive_dbt_queries \
        --args '{lookback_days: 14, top_n: 50, credit_rate_usd: 3, high_cost_threshold_usd: 25000}'

    [TODO] Persist results to a model so we can track cost history over time.
    [TODO] Cross-reference with find_spillage_candidates output to flag queries
           that are expensive AND spilling — those are the highest-leverage refactors.
  --#}

  {% if execute %}

    {{ log("--- Starting Expensive dbt Query Analysis ---", info=true) }}
    {{ log("Criteria: top " ~ top_n ~ " dbt queries by projected annual cost, with >= " ~ min_total_credits ~ " credits consumed in last " ~ lookback_days ~ " days.", info=true) }}
    {{ log("Credit rate: $" ~ credit_rate_usd ~ "/credit. High-cost threshold: $" ~ high_cost_threshold_usd ~ "/yr.", info=true) }}

    {% set performance_sql %}
      with dbt_sessions as (
          {{ dbt_cost_optimization_package.dbt_session_filter(lookback_days=lookback_days) }}
      ),

      query_costs as (
          select
              qh.query_hash,
              any_value(qh.query_id) as sample_query_id,
              substring(any_value(qh.query_text), 1, 200) as query_text_preview,
              {#- extract dbt node_id from the JSON comment dbt prepends to compiled queries -#}
              parse_json(
                  regexp_substr(any_value(qh.query_text), '/\\*\\s+(\\{.*?\\})\\s+\\*/', 1, 1, 'e', 1)
              ):node_id::string as dbt_node_id,
              any_value(qh.warehouse_name) as warehouse_name,
              any_value(qh.warehouse_size) as warehouse_size,
              count(distinct qh.query_id) as total_runs,
              round(sum(qah.credits_attributed_compute), 4) as total_credits_in_window,
              round(avg(qh.total_elapsed_time / 1000), 2) as avg_elapsed_sec
          from
              snowflake.account_usage.query_history qh
          inner join
              dbt_sessions s on qh.session_id = s.session_id
          inner join
              snowflake.account_usage.query_attribution_history qah on qh.query_id = qah.query_id
          where
              qh.start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
              and qah.credits_attributed_compute > 0
          group by
              qh.query_hash
          having
              sum(qah.credits_attributed_compute) >= {{ min_total_credits }}
      ),

      projected_costs as (
          select
              query_hash,
              sample_query_id,
              query_text_preview,
              dbt_node_id,
              warehouse_name,
              warehouse_size,
              total_runs,
              avg_elapsed_sec,
              total_credits_in_window,
              round(total_credits_in_window / {{ lookback_days }}, 4) as avg_daily_credits,
              round((total_credits_in_window / {{ lookback_days }}) * 365, 2) as estimated_annual_credits,
              round((total_credits_in_window / {{ lookback_days }}) * 365 * {{ credit_rate_usd }}, 2) as estimated_annual_cost_usd
          from
              query_costs
      )

      {#-
        recommendation_key encodes tier logic in SQL — string equality compares
        reliably across types in minijinja.
      -#}
      select
          query_hash,
          sample_query_id,
          query_text_preview,
          dbt_node_id,
          warehouse_name,
          warehouse_size,
          total_runs,
          avg_elapsed_sec,
          total_credits_in_window,
          avg_daily_credits,
          estimated_annual_credits,
          estimated_annual_cost_usd,
          case
              when estimated_annual_cost_usd > {{ high_cost_threshold_usd }} then 'high_cost'
              else 'tracked'
          end as recommendation_key
      from
          projected_costs
      order by
          estimated_annual_cost_usd desc
      limit {{ top_n }}
    {% endset %}

    {% set results = run_query(performance_sql) %}

    {% if results.rows | length == 0 %}
        {{ log("No queries met the criteria. Try lowering min_total_credits or extending lookback_days.", info=true) }}
    {% else %}

        {{ log("Found " ~ results.rows | length ~ " expensive query/queries.", info=true) }}
        {{ log("", info=true) }}

        {% set recommendations = [] %}

        {% for row in results.rows %}
            {% set query_hash = row["QUERY_HASH"] %}
            {% set sample_query_id = row["SAMPLE_QUERY_ID"] %}
            {% set query_text_preview = row["QUERY_TEXT_PREVIEW"] %}
            {% set dbt_node_id = row["DBT_NODE_ID"] %}
            {% set warehouse_name = row["WAREHOUSE_NAME"] %}
            {% set warehouse_size = row["WAREHOUSE_SIZE"] %}
            {% set total_runs = row["TOTAL_RUNS"] %}
            {% set avg_elapsed_sec = row["AVG_ELAPSED_SEC"] %}
            {% set total_credits_in_window = row["TOTAL_CREDITS_IN_WINDOW"] %}
            {% set avg_daily_credits = row["AVG_DAILY_CREDITS"] %}
            {% set estimated_annual_credits = row["ESTIMATED_ANNUAL_CREDITS"] %}
            {% set estimated_annual_cost_usd = row["ESTIMATED_ANNUAL_COST_USD"] %}
            {% set recommendation_key = row["RECOMMENDATION_KEY"] %}

            {# look up the dbt model from graph.nodes when we have a node_id from the query header #}
            {% set model_fqn = none %}
            {% set current_materialization = none %}
            {% if dbt_node_id and dbt_node_id in graph.nodes %}
                {% set model_node = graph.nodes[dbt_node_id] %}
                {% set model_fqn = model_node.database ~ "." ~ model_node.schema ~ "." ~ (model_node.alias | default(model_node.name)) %}
                {% set current_materialization = model_node.config.materialized %}
            {% endif %}

            {% set recommendation = none %}
            {% set reason = none %}
            {% set severity = 'info' %}

            {% if recommendation_key == 'high_cost' %}
                {% set recommendation = 'Review query for refactor opportunities (high projected cost)' %}
                {% set reason = 'Projected $' ~ estimated_annual_cost_usd ~ '/yr (' ~ estimated_annual_credits ~ ' credits/yr) exceeds the $' ~ high_cost_threshold_usd ~ ' threshold. Ran ' ~ total_runs ~ ' times over ' ~ lookback_days ~ ' days at avg ' ~ avg_elapsed_sec ~ 's per run.' %}
                {% set severity = 'warn' %}
            {% else %}
                {% set recommendation = 'Monitor — recurring credit consumption' %}
                {% set reason = 'Projected $' ~ estimated_annual_cost_usd ~ '/yr (' ~ estimated_annual_credits ~ ' credits/yr). Ran ' ~ total_runs ~ ' times over ' ~ lookback_days ~ ' days at avg ' ~ avg_elapsed_sec ~ 's per run.' %}
            {% endif %}

            {# Skip queries not in the current project when dbt_project_only is true #}
            {% if dbt_project_only and model_fqn is none %}
                {# skip — not a model in this project #}
            {% else %}

            {% do recommendations.append({
                'query_hash': query_hash,
                'sample_query_id': sample_query_id,
                'query_text_preview': query_text_preview,
                'dbt_node_id': dbt_node_id,
                'model_fqn': model_fqn,
                'current_materialization': current_materialization,
                'warehouse_name': warehouse_name,
                'warehouse_size': warehouse_size,
                'total_runs': total_runs,
                'avg_elapsed_sec': avg_elapsed_sec,
                'total_credits_in_window': total_credits_in_window,
                'avg_daily_credits': avg_daily_credits,
                'estimated_annual_credits': estimated_annual_credits,
                'estimated_annual_cost_usd': estimated_annual_cost_usd,
                'recommendation': recommendation,
                'reason': reason,
                'severity': severity
            }) %}

            {% endif %}
        {% endfor %}

        {# warns first; SQL already ordered by estimated_annual_cost_usd desc, preserving that within buckets #}
        {% set warns = recommendations | selectattr('severity', 'equalto', 'warn') | list %}
        {% set infos = recommendations | selectattr('severity', 'equalto', 'info') | list %}
        {% set sorted_recs = warns + infos %}

        {{ log("--- Recommendations ---", info=true) }}

        {% for r in sorted_recs %}
            {% set title = 'Model: ' ~ r.model_fqn ~ ' (' ~ r.current_materialization ~ ')' if r.model_fqn else 'Query hash: ' ~ r.query_hash %}
            {{ dbt_cost_optimization_package.log_recommendation(
                title=title,
                recommendation=r.recommendation,
                reason=r.reason,
                severity=r.severity,
                metrics={
                    'warehouse': r.warehouse_name ~ ' (' ~ r.warehouse_size ~ ')',
                    'total_runs': r.total_runs,
                    'avg_elapsed_sec': r.avg_elapsed_sec,
                    'total_credits_in_window': r.total_credits_in_window,
                    'avg_daily_credits': r.avg_daily_credits,
                    'estimated_annual_credits': r.estimated_annual_credits,
                    'estimated_annual_cost_usd': '$' ~ r.estimated_annual_cost_usd,
                    'query_hash': r.query_hash,
                    'sample_query_id': r.sample_query_id,
                    'query_text_preview': r.query_text_preview
                }
            ) }}
        {% endfor %}

    {% endif %}

  {% endif %}

{% endmacro %}
