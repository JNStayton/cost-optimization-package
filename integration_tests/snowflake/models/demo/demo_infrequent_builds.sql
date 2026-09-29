{{ config(materialized='table') }}
-- Incremental slice: few builds (fixture) and a unique RECORD_ID, so the key probe lifts
-- confidence from 50 to 60, which moves the status to actionable_review.
select
    seq4()                                                    as record_id,
    dateadd(minute, -seq4(), current_timestamp())::timestamp_ntz as updated_at
from table(generator(rowcount => 1000))
