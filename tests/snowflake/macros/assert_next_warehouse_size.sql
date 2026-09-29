{#--
  next_warehouse_size steps a warehouse size one rung up or down the Snowflake size
  ladder. It accepts both formats Snowflake uses: SHOW WAREHOUSES / QUERY_HISTORY
  ('X-Small', '2X-Large') and WAREHOUSE_EVENTS_HISTORY ('XSMALL', 'XXLARGE', 'X4LARGE').
  It returns null past either end of the ladder and for unknown or null sizes, so no
  DDL is generated. Returns rows only on mismatch.
--#}
with cases as (
    select 'X-Small' as size, 'SMALL' as expected_up, null as expected_down
    union all select 'XSMALL',   'SMALL',    null
    union all select 'Small',    'MEDIUM',   'X-SMALL'
    union all select 'Medium',   'LARGE',    'SMALL'
    union all select 'Large',    'XLARGE',   'MEDIUM'
    union all select 'X-Large',  '2X-LARGE', 'LARGE'
    union all select 'XLARGE',   '2X-LARGE', 'LARGE'
    union all select '2X-Large', '3X-LARGE', 'XLARGE'
    union all select 'XXLARGE',  '3X-LARGE', 'XLARGE'
    union all select '3X-Large', '4X-LARGE', '2X-LARGE'
    union all select 'XXXLARGE', '4X-LARGE', '2X-LARGE'
    union all select '4X-Large', '5X-LARGE', '3X-LARGE'
    union all select 'X4LARGE',  '5X-LARGE', '3X-LARGE'
    union all select '5X-Large', '6X-LARGE', '4X-LARGE'
    union all select 'X5LARGE',  '6X-LARGE', '4X-LARGE'
    union all select '6X-Large', null,       '5X-LARGE'
    union all select 'X6LARGE',  null,       '5X-LARGE'
    union all select 'unknown',  null,       null
    union all select null,       null,       null
),

results as (
    select
        size, expected_up, expected_down,
        {{ next_warehouse_size('size', 'up') }}   as got_up,
        {{ next_warehouse_size('size', 'down') }} as got_down
    from cases
)

select * from results
where got_up is distinct from expected_up
   or got_down is distinct from expected_down
