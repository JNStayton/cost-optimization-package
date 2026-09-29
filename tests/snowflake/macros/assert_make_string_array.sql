{#--
  make_string_array builds a Snowflake array from a list of strings, including the
  empty list. Returns rows only on mismatch.
--#}
with cases as (
    select 'two values' as case_name, {{ make_string_array(['a', 'b']) }} as arr, 2 as expected_size, 'a|b' as expected_joined
    union all select 'one value',     {{ make_string_array(['model.pkg.x']) }},    1, 'model.pkg.x'
    union all select 'empty list',    {{ make_string_array([]) }},                 0, ''
)
select case_name, array_size(arr) as got_size, array_to_string(arr, '|') as got_joined
from cases
where array_size(arr) != expected_size
   or array_to_string(arr, '|') != expected_joined
