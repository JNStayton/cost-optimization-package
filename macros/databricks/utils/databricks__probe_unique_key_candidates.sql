{% macro databricks__probe_unique_key_candidates() %}

  {#--
    Post-hook for fct_databricks__incremental_model_candidates.

    For each candidate table, probes the unique_key_candidates array using
    approx_count_distinct to find the first column whose cardinality is
    approximately equal to the row count (>= threshold, accounting for the
    HyperLogLog error margin). All candidate columns for a table are probed
    in a single scan to minimise compute cost.

    When a likely unique key is confirmed it updates:
      - likely_unique_key       — the confirmed column name
      - validate_uniqueness_sql — re-pointed to the confirmed column
      - updated_model_config    — unique_key swapped to the confirmed column

    When no single-column unique key is found and the initial strategy was
    delete+insert or merge, downgrades to append and updates:
      - suggested_incremental_strategy → 'append'
      - strategy_notes                 → explains the downgrade and next steps
      - updated_model_config           → rebuilt as an append config template
      - incremental_filter_template    → rebuilt as a max(filter_col) where-clause
                                         when a filter column exists; otherwise a
                                         TODO note

    append is the safer default when no unique key is confirmed: its failure
    mode (visible duplicates) is preferable to silent data corruption from
    merge or delete+insert on an unconfirmed key.

    Only rows from the current snapshot (snapshot_date = current_date()) are
    updated.

    Variables:
      incremental_unique_key_probe_threshold (default 0.95)
  --#}

  {% if execute and target.type == 'databricks' %}

    {% set threshold = var('incremental_unique_key_probe_threshold', 0.95) %}

    {{ log("probe_unique_key_candidates: starting uniqueness probe...", info=true) }}

    {% set candidates_sql %}
      select
        table_fqn,
        suggested_unique_key,
        array_join(unique_key_candidates, ',')         as candidates_csv,
        suggested_incremental_strategy                  as current_strategy,
        lower(suggested_filter_column)                  as suggested_filter_column
      from {{ this }}
      where unique_key_candidates is not null
        and size(unique_key_candidates) > 0
        and snapshot_date = current_date()
    {% endset %}

    {% set candidates = run_query(candidates_sql) %}

    {% if candidates and candidates.rows | length > 0 %}

      {% for row in candidates %}

        {% set table_fqn      = row['table_fqn'] %}
        {% set best_conv_key  = row['suggested_unique_key'] %}
        {% set candidates_csv = row['candidates_csv'] %}
        {% set candidate_cols = candidates_csv.split(',') %}
        {% set current_strat  = row['current_strategy'] %}
        {% set filter_col     = row['suggested_filter_column'] if row['suggested_filter_column'] else none %}

        {{ log("probe_unique_key_candidates: probing " ~ (candidate_cols | length) ~ " candidate(s) for " ~ table_fqn, info=true) }}

        {# Scan the table once — one approx_count_distinct per candidate column #}
        {% set probe_sql %}
          select
            count(*) as total_rows
            {% for col in candidate_cols %}
              , approx_count_distinct({{ adapter.quote(col) }}) as col_{{ loop.index }}_distinct
            {% endfor %}
          from {{ table_fqn }}
        {% endset %}

        {% set probe_result = run_query(probe_sql) %}
        {% set total_rows = probe_result.rows[0][0] | int %}

        {# Walk candidates in rank order; stop at the first likely-unique column. #}
        {% set ns = namespace(confirmed_key=none) %}

        {% for col in candidate_cols %}
          {% if ns.confirmed_key is none %}
            {% set approx_distinct = probe_result.rows[0][loop.index] | int %}
            {% if total_rows > 0 and (approx_distinct / total_rows) >= threshold %}
              {% set ns.confirmed_key = col | lower %}
            {% endif %}
          {% endif %}
        {% endfor %}

        {% if ns.confirmed_key is not none %}

          {% set new_validate_sql = 'select count(*) = count(distinct ' ~ ns.confirmed_key ~ ') as is_unique from ' ~ table_fqn | lower %}
          {% set old_key_in_template = (best_conv_key | lower) if best_conv_key else '-- TODO: add your surrogate key' %}

          {% set update_sql %}
            update {{ this }}
            set
              likely_unique_key       = '{{ ns.confirmed_key }}',
              validate_uniqueness_sql = '{{ new_validate_sql }}',
              updated_model_config    = replace(
                updated_model_config,
                'unique_key=''{{ old_key_in_template }}''',
                'unique_key=''{{ ns.confirmed_key }}'''
              )
            where table_fqn = '{{ table_fqn }}'
              and snapshot_date = current_date()
          {% endset %}

          {% do run_query(update_sql) %}
          {{ log("probe_unique_key_candidates: confirmed '" ~ ns.confirmed_key ~ "' as likely unique key for " ~ table_fqn, info=true) }}

        {% else %}

          {{ log("probe_unique_key_candidates: no single-column unique key found for " ~ table_fqn ~ " — composite key likely needed", info=true) }}

          {# Downgrade delete+insert or merge to append when no unique key is confirmed.
             Using an unconfirmed key with merge/delete+insert risks silent data corruption
             (phantom deletes or missed upserts); append failure mode (visible duplicates)
             is the safer default. strategy_notes guides the user to the correct next step. #}
          {% if current_strat in ('delete+insert', 'merge') %}

            {% if filter_col %}
              {% set update_sql %}
                update {{ this }}
                set
                  suggested_incremental_strategy = 'append',
                  strategy_notes = 'No single-column unique key confirmed by cardinality probe — strategy downgraded from {{ current_strat }} to append. To implement a scoped strategy: (1) generate a surrogate key with dbt_utils.generate_surrogate_key([<grain_columns>]) and configure unique_key on that column, then re-evaluate for merge or delete+insert; or (2) use incremental_predicates with delete+insert to scope deletes to the {{ filter_col }} window if records arrive cleanly with no late-arriving data outside the window.',
                  updated_model_config =
                      '{'||'{'||' config('||chr(10)
                    ||'    materialized=''incremental'','||chr(10)
                    ||'    incremental_strategy=''append'','||chr(10)
                    ||'    on_schema_change=''append_new_columns'''||chr(10)
                    ||') '||'}'||'}',
                  incremental_filter_template =
                      '{'||'%'||' if is_incremental() '||'%'||'}'||chr(10)
                    ||'    where {{ filter_col }} > (select max({{ filter_col }}) from '
                    ||'{'||'{'||' this '||'}'||'}'||')'||chr(10)
                    ||'{'||'%'||' endif '||'%'||'}'
                where table_fqn = '{{ table_fqn }}'
                  and snapshot_date = current_date()
              {% endset %}
            {% else %}
              {% set update_sql %}
                update {{ this }}
                set
                  suggested_incremental_strategy = 'append',
                  strategy_notes = 'No single-column unique key confirmed by cardinality probe — strategy downgraded from {{ current_strat }} to append. To implement a scoped strategy: generate a surrogate key with dbt_utils.generate_surrogate_key([<grain_columns>]) and configure unique_key on that column, then re-evaluate for merge or delete+insert.',
                  updated_model_config =
                      '{'||'{'||' config('||chr(10)
                    ||'    materialized=''incremental'','||chr(10)
                    ||'    incremental_strategy=''append'','||chr(10)
                    ||'    on_schema_change=''append_new_columns'''||chr(10)
                    ||') '||'}'||'}',
                  incremental_filter_template = '-- No suitable incremental filter column detected. Add a where clause manually.'
                where table_fqn = '{{ table_fqn }}'
                  and snapshot_date = current_date()
              {% endset %}
            {% endif %}

            {% do run_query(update_sql) %}
            {{ log("probe_unique_key_candidates: downgraded " ~ table_fqn ~ " from " ~ current_strat ~ " to append — no confirmed unique key", info=true) }}

          {% endif %}

        {% endif %}

      {% endfor %}

    {% else %}
      {{ log("probe_unique_key_candidates: no candidates with unique key candidates found, skipping.", info=true) }}
    {% endif %}

  {% endif %}

{% endmacro %}
