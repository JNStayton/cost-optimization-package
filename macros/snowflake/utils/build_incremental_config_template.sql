{#--
  Generates a copy-pasteable dbt config template string for incremental models.

  Called from: fct_snowflake__incremental_config_recommendations

  Returns: A SQL CASE expression that builds a multi-line string containing a
  ready-to-use dbt model config block + is_incremental() filter. The output is
  stored in the `dbt_config_template` column for end-users to copy into their models.

  Column contract (must exist in the calling CTE):
    - incremental_strategy  (text: 'merge', 'delete+insert', 'microbatch', 'append')
    - suggested_filter_column (text or null: timestamp/date column for is_incremental() filter)
    - best_unique_key (text or null: detected unique key candidate)

  Pattern notes:
    - Uses {% raw %}...{% endraw %} inline to emit Jinja delimiters ({{ }}, {% %})
      as literal SQL string content without dbt interpreting them at compile time.
    - Uses chr(10) for actual newline characters in the stored string output.
    - No macro arguments — column names are referenced directly in the emitted SQL.
      If adapting for other platforms, parameterize the column names and strategy values.

  Example output (stored in column, with actual newlines):
    {{
      config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id',
        on_schema_change='append_new_columns'
      )
    }}

    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
--#}

{% macro build_incremental_config_template() %}
case
    when incremental_strategy = 'microbatch'
        then
            {% raw %}'{{' || chr(10){% endraw %}
            || '  config(' || chr(10)
            || '    materialized=''incremental'',' || chr(10)
            || '    incremental_strategy=''microbatch'',' || chr(10)
            || '    event_time=''' || lower(suggested_filter_column) || ''',' || chr(10)
            || '    batch_size=''day'',' || chr(10)
            || '    begin=''YYYY-MM-DD''  -- set to earliest date to backfill' || chr(10)
            || '  )' || chr(10)
            || {% raw %}'}}' {% endraw %}

    when incremental_strategy in ('merge', 'delete+insert')
        and suggested_filter_column is not null
        then
            {% raw %}'{{' || chr(10){% endraw %}
            || '  config(' || chr(10)
            || '    materialized=''incremental'',' || chr(10)
            || '    incremental_strategy=''' || incremental_strategy || ''',' || chr(10)
            || '    unique_key=''' || lower(coalesce(best_unique_key, '<unique_key>')) || ''',' || chr(10)
            || '    on_schema_change=''append_new_columns''' || chr(10)
            || '  )' || chr(10)
            || {% raw %}'}}'{% endraw %} || chr(10) || chr(10)
            || {% raw %}'{% if is_incremental() %}'{% endraw %} || chr(10)
            || 'where ' || lower(suggested_filter_column)
            || ' > (select max(' || lower(suggested_filter_column) || ') from '
            || {% raw %}'{{ this }}'{% endraw %} || ')' || chr(10)
            || {% raw %}'{% endif %}'{% endraw %}

    when incremental_strategy in ('merge', 'delete+insert')
        then
            {% raw %}'{{' || chr(10){% endraw %}
            || '  config(' || chr(10)
            || '    materialized=''incremental'',' || chr(10)
            || '    incremental_strategy=''' || incremental_strategy || ''',' || chr(10)
            || '    unique_key=''' || lower(coalesce(best_unique_key, '<unique_key>')) || ''',' || chr(10)
            || '    on_schema_change=''append_new_columns''' || chr(10)
            || '  )' || chr(10)
            || {% raw %}'}}'{% endraw %} || chr(10) || chr(10)
            || '-- TODO: add a filter column (timestamp/date) to scope incremental loads'

    when incremental_strategy = 'append' and suggested_filter_column is not null
        then
            {% raw %}'{{' || chr(10){% endraw %}
            || '  config(' || chr(10)
            || '    materialized=''incremental'',' || chr(10)
            || '    incremental_strategy=''append''' || chr(10)
            || '  )' || chr(10)
            || {% raw %}'}}'{% endraw %} || chr(10) || chr(10)
            || {% raw %}'{% if is_incremental() %}'{% endraw %} || chr(10)
            || 'where ' || lower(suggested_filter_column)
            || ' > (select max(' || lower(suggested_filter_column) || ') from '
            || {% raw %}'{{ this }}'{% endraw %} || ')' || chr(10)
            || {% raw %}'{% endif %}'{% endraw %}

    else
            {% raw %}'{{' || chr(10){% endraw %}
            || '  config(' || chr(10)
            || '    materialized=''incremental'',' || chr(10)
            || '    incremental_strategy=''append''' || chr(10)
            || '  )' || chr(10)
            || {% raw %}'}}'{% endraw %} || chr(10) || chr(10)
            || '-- TODO: add a filter column (timestamp/date) to scope incremental loads' || chr(10)
            || '-- TODO: verify data is truly append-only before using this strategy'
end
{% endmacro %}
