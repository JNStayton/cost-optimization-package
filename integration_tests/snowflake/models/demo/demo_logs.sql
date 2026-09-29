{{ config(materialized='table') }}
-- Incremental slice: a timestamp column but no key-like column, so the proposed strategy is append.
select
    dateadd(minute, -seq4(), current_timestamp())::timestamp_ntz as logged_at,
    'log line ' || seq4()                                     as message
from table(generator(rowcount => 1000))
