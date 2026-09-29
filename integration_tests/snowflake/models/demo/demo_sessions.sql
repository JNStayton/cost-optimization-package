{{ config(materialized='table') }}
-- Incremental slice: SESSION_ID and USER_ID both repeat, so no key passes the uniqueness probe.
select
    seq4() % 500                                              as session_id,
    seq4() % 50                                               as user_id,
    dateadd(minute, -seq4(), current_timestamp())::timestamp_ntz as started_at
from table(generator(rowcount => 1000))
