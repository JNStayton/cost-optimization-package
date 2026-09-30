{{ config(alias='query_attribution_history') }}
{#- Stand-in for ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY. Types come from the real view.
    Only the 60 demo_slow_view reads (FIXTURE_ANALYST, a non-dbt session) have rows,
    0.001 credits each: user cost attribution must use these attributed credits for
    them (0.06 in total, not 60 x 45 s at the list rate) and set
    credits_from_attribution. Every other query has no row, so the package falls back
    to elapsed time x list rate (the expensive-query path included). -#}
select * from snowflake.account_usage.query_attribution_history where false
union all
select
    'demo_slow_view_q' || seq4()                              as query_id,
    null                                                      as parent_query_id,
    'demo_slow_view_q' || seq4()                              as root_query_id,
    null                                                      as warehouse_id,
    'FIXTURE_WH'                                              as warehouse_name,
    'hash_demo_slow_view'                                     as query_hash,
    'phash_demo_slow_view'                                    as query_parameterized_hash,
    null                                                      as query_tag,
    'FIXTURE_ANALYST'                                         as user_name,
    dateadd(hour, -(seq4() + 1), current_timestamp())         as start_time,
    dateadd(hour, -(seq4() + 1), current_timestamp())         as end_time,
    0.001                                                     as credits_attributed_compute,
    0                                                         as credits_used_query_acceleration
from table(generator(rowcount => 60))
