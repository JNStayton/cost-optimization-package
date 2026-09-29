{{ config(materialized='table') }}
-- Incremental slice: rows triple on every build (fixture), so rebuild redundancy is low.
select
    seq4()                                                    as row_id,
    dateadd(minute, -seq4(), current_timestamp())::timestamp_ntz as loaded_at
from table(generator(rowcount => 1000))
