{#--
  Returns a SQL case expression: the scaling efficiency of the step up from a warehouse
  size to the next. 1.0 means doubling the size exactly halves the runtime (same
  credits); 0.86 means the runtime falls to 1 / (2 x 0.86) = 58% and credits rise by
  1 / 0.86 - 1 = 16%.

  Source: Snowflake Summit session "Beyond Code: Right-Sizing Your Warehouse" (one
  complex query run on every size, X-Small through 6X-Large). Its "100%" steps at two
  minutes and under are treated as rounding, so 3X-Large through 5X-Large take the
  0.81 measured above them. Our own calibration on spilling dbt builds measured 0.91 to
  0.97 for X-Small → Small.

  Handles both SHOW WAREHOUSES format ('X-Small', '2X-Large') and
  WAREHOUSE_EVENTS_HISTORY format ('XSMALL', 'XXLARGE'). Returns null for 6X-Large
  (no larger size) and unknown sizes.

  Usage:
    {{ warehouse_scale_up_efficiency('warehouse_current_size') }}
--#}

{% macro warehouse_scale_up_efficiency(size_column) %}
  case lower({{ size_column }})
    when 'x-small'  then 1.00
    when 'xsmall'   then 1.00
    when 'small'    then 1.00
    when 'medium'   then 0.86
    when 'large'    then 0.92
    when 'x-large'  then 0.86
    when 'xlarge'   then 0.86
    when '2x-large' then 0.85
    when 'xxlarge'  then 0.85
    when '3x-large' then 0.81
    when 'xxxlarge' then 0.81
    when '4x-large' then 0.81
    when 'x4large'  then 0.81
    when '5x-large' then 0.81
    when 'x5large'  then 0.81
    else null
  end
{% endmacro %}
