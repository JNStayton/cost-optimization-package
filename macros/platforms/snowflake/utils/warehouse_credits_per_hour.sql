{#--
  Returns a SQL case expression that maps a warehouse size column to Snowflake's
  published credits-per-hour rate (X-Small=1 through 6X-Large=512).

  Handles both SHOW WAREHOUSES format (title case: 'X-Small', '2X-Large')
  and WAREHOUSE_EVENTS_HISTORY format (uppercase: 'XSMALL', 'XXLARGE')
  by lowercasing before comparison.

  Returns null for unknown sizes. Callers should wrap with
  coalesce(..., 1) to fall back to X-Small rate.

  Usage:
    {{ warehouse_credits_per_hour('current_size') }}
    coalesce({{ warehouse_credits_per_hour('wc.current_size') }}, 1)
--#}

{% macro warehouse_credits_per_hour(size_column) %}
  case lower({{ size_column }})
    when 'x-small'  then 1
    when 'xsmall'   then 1
    when 'small'    then 2
    when 'medium'   then 4
    when 'large'    then 8
    when 'x-large'  then 16
    when 'xlarge'   then 16
    when '2x-large' then 32
    when 'xxlarge'  then 32
    when '3x-large' then 64
    when 'xxxlarge' then 64
    when '4x-large' then 128
    when 'x4large'  then 128
    when '5x-large' then 256
    when 'x5large'  then 256
    when '6x-large' then 512
    when 'x6large'  then 512
    else null
  end
{% endmacro %}
