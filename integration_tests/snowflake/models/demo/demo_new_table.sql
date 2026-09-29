{{ config(materialized='table') }}
-- Incremental slice: only two builds recorded (fixture), so history is insufficient.
select
    seq4()                                                    as row_id,
    dateadd(minute, -seq4(), current_timestamp())::timestamp_ntz as loaded_at
from table(generator(rowcount => 1000))
