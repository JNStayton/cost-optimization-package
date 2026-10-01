{#--
  warehouse_scale_up_efficiency gives the benchmark efficiency of the step up from every
  warehouse size, in both size formats, and null for 6X-Large (nothing larger) and
  unknown or null sizes. Returns rows only on mismatch.
--#}
with cases as (
    select 'X-Small' as size, 1.00 as expected
    union all select 'XSMALL',   1.00
    union all select 'Small',    1.00
    union all select 'Medium',   0.86
    union all select 'Large',    0.92
    union all select 'X-Large',  0.86
    union all select 'XLARGE',   0.86
    union all select '2X-Large', 0.85
    union all select 'XXLARGE',  0.85
    union all select '3X-Large', 0.81
    union all select 'XXXLARGE', 0.81
    union all select '4X-Large', 0.81
    union all select 'X4LARGE',  0.81
    union all select '5X-Large', 0.81
    union all select 'X5LARGE',  0.81
    union all select '6X-Large', null
    union all select 'X6LARGE',  null
    union all select 'unknown',  null
    union all select null,       null
),

results as (
    select size, expected, {{ warehouse_scale_up_efficiency('size') }} as got
    from cases
)

select * from results
where got is distinct from expected
