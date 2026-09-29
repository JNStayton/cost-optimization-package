{#--
  Returns a SQL case expression that maps a warehouse size to its next size
  up or down in the Snowflake size ladder.

  Handles both SHOW WAREHOUSES format (title case: 'X-Small', '2X-Large')
  and WAREHOUSE_EVENTS_HISTORY format (uppercase: 'XSMALL', 'XXLARGE')
  by lowercasing before comparison.

  Returns null for unknown/unresolvable sizes (top of ladder for up,
  bottom for down). This nulls out any concatenated DDL string, which is
  correct — no DDL is safer than wrong DDL.

  Usage:
    {{ next_warehouse_size('warehouse_size', 'up') }}
    {{ next_warehouse_size('warehouse_current_size', 'down') }}
--#}

{% macro next_warehouse_size(size_column, direction='up') %}
  case lower({{ size_column }})
    {% if direction == 'up' %}
      when 'x-small'  then 'SMALL'
      when 'xsmall'   then 'SMALL'
      when 'small'    then 'MEDIUM'
      when 'medium'   then 'LARGE'
      when 'large'    then 'XLARGE'
      when 'x-large'  then '2X-LARGE'
      when 'xlarge'   then '2X-LARGE'
      when '2x-large' then '3X-LARGE'
      when 'xxlarge'  then '3X-LARGE'
      when '3x-large' then '4X-LARGE'
      when 'xxxlarge' then '4X-LARGE'
      when '4x-large' then '5X-LARGE'
      when 'x4large'  then '5X-LARGE'
      when '5x-large' then '6X-LARGE'
      when 'x5large'  then '6X-LARGE'
      else null
    {% else %}
      when 'small'    then 'X-SMALL'
      when 'medium'   then 'SMALL'
      when 'large'    then 'MEDIUM'
      when 'x-large'  then 'LARGE'
      when 'xlarge'   then 'LARGE'
      when '2x-large' then 'XLARGE'
      when 'xxlarge'  then 'XLARGE'
      when '3x-large' then '2X-LARGE'
      when 'xxxlarge' then '2X-LARGE'
      when '4x-large' then '3X-LARGE'
      when 'x4large'  then '3X-LARGE'
      when '5x-large' then '4X-LARGE'
      when 'x5large'  then '4X-LARGE'
      when '6x-large' then '5X-LARGE'
      when 'x6large'  then '5X-LARGE'
      else null
    {% endif %}
  end
{% endmacro %}
