{% macro probe_unique_key_candidates() %}

  {#--
    Post-hook for fct_snowflake__incremental_config_recommendations.

    For each candidate table with a proposed merge strategy, runs an EXACT
    uniqueness probe: count(*) = count(distinct key) AND count_if(key IS NULL) = 0.

    When a key is confirmed:
      - likely_unique_key set to the confirmed column
      - identified_unique_key set (merge strategy only)
      - confidence_score increased by 10 (validated signal)
      - blocking_signals: removes 'key_pending_exact_validation'
      - dbt_model_config: unique_key parameter updated

    When no single-column key passes:
      - strategy downgraded to investigate (recommendation_status updated)
      - confidence_score decreased by 30
      - blocking_signals: adds 'key_not_exact_or_nullable'
      - identified_unique_key cleared

    Only probes tables where incremental_strategy = 'merge' (append doesn't
    require a key). This keeps compute cost proportional to actionable candidates.
  --#}

  {% if execute and target.type == 'snowflake' %}

    {{ log("probe_unique_key_candidates: starting exact uniqueness probe...", info=true) }}

    {% set candidates_sql %}
      select
        table_fqn,
        best_unique_key,
        array_to_string(unique_key_candidates, ',') as candidates_csv,
        incremental_strategy,
        lower(suggested_filter_column)              as suggested_filter_column,
        confidence_score
      from {{ this }}
      where unique_key_candidates is not null
        and array_size(unique_key_candidates) > 0
        and incremental_strategy = 'merge'
    {% endset %}

    {% set candidates = run_query(candidates_sql) %}

    {% if candidates and candidates.rows | length > 0 %}

      {% for row in candidates %}

        {% set table_fqn      = row['TABLE_FQN'] %}
        {% set best_conv_key  = row['BEST_UNIQUE_KEY'] %}
        {% set candidates_csv = row['CANDIDATES_CSV'] %}
        {% set candidate_cols = candidates_csv.split(',') %}
        {% set current_strat  = row['INCREMENTAL_STRATEGY'] %}
        {% set filter_col     = row['SUGGESTED_FILTER_COLUMN'] if row['SUGGESTED_FILTER_COLUMN'] else none %}
        {% set current_score  = row['CONFIDENCE_SCORE'] | int %}

        {{ log("probe_unique_key_candidates: exact probe for " ~ (candidate_cols | length) ~ " candidate(s) on " ~ table_fqn, info=true) }}

        {# Probe each candidate with exact count distinct + null check #}
        {% set ns = namespace(confirmed_key=none) %}

        {% for col in candidate_cols %}
          {% if ns.confirmed_key is none %}
            {% set probe_sql %}
              select
                count(*) as total_rows,
                count(distinct {{ adapter.quote(col) }}) as distinct_count,
                count_if({{ adapter.quote(col) }} is null) as null_count
              from {{ table_fqn }}
            {% endset %}

            {% set probe_result = run_query(probe_sql) %}
            {% set total_rows    = probe_result.rows[0][0] | int %}
            {% set distinct_count = probe_result.rows[0][1] | int %}
            {% set null_count    = probe_result.rows[0][2] | int %}

            {% if total_rows > 0 and distinct_count == total_rows and null_count == 0 %}
              {% set ns.confirmed_key = col | lower %}
            {% endif %}
          {% endif %}
        {% endfor %}

        {% if ns.confirmed_key is not none %}

          {% set old_key_in_template = (best_conv_key | lower) if best_conv_key else '<unique_key>' %}
          {% set new_score = [current_score + 10, 100] | min %}

          {% set update_sql %}
            update {{ this }}
            set
              likely_unique_key     = '{{ ns.confirmed_key }}',
              identified_unique_key = '{{ ns.confirmed_key }}',
              confidence_score      = {{ new_score }},
              blocking_signals      = array_except(blocking_signals, array_construct('key_pending_exact_validation')),
              dbt_model_config      = replace(
                dbt_model_config,
                'unique_key=''' || '{{ old_key_in_template }}' || '''',
                'unique_key=''' || '{{ ns.confirmed_key }}' || ''''
              )
            where table_fqn = '{{ table_fqn }}'
          {% endset %}

          {% do run_query(update_sql) %}
          {{ log("probe_unique_key_candidates: CONFIRMED '" ~ ns.confirmed_key ~ "' (exact unique, zero nulls) for " ~ table_fqn, info=true) }}

        {% else %}

          {{ log("probe_unique_key_candidates: FAILED — no exact unique key for " ~ table_fqn, info=true) }}

          {# Key probe failed: downgrade confidence and update status #}
          {% set new_score = [current_score - 30, 0] | max %}

          {% set update_sql %}
            update {{ this }}
            set
              confidence_score      = {{ new_score }},
              recommendation_status = case
                when {{ new_score }} >= 60 then 'actionable_review'
                when {{ new_score }} >= 30 then 'investigate'
                else 'do_not_recommend'
              end,
              blocking_signals      = array_cat(
                array_except(blocking_signals, array_construct('key_pending_exact_validation')),
                array_construct('key_not_exact_or_nullable')
              ),
              identified_unique_key = null,
              strategy_notes        = 'Key probe failed: no single-column candidate passed exact uniqueness '
                || '(count(*) = count(distinct key) AND null_count = 0). '
                || 'Consider a composite surrogate key via dbt_utils.generate_surrogate_key([<grain_columns>]).'
            where table_fqn = '{{ table_fqn }}'
          {% endset %}

          {% do run_query(update_sql) %}

        {% endif %}

      {% endfor %}

    {% else %}
      {{ log("probe_unique_key_candidates: no merge candidates with key candidates found, skipping.", info=true) }}
    {% endif %}

  {% endif %}

{% endmacro %}
