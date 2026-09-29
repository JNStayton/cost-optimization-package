{{ config(materialized='table') }}
-- Incremental slice: ORDER_ID is unique and never null, so probe_unique_key_candidates confirms it.
select
    seq4()                                                    as order_id,
    seq4() % 100                                              as customer_id,
    uniform(1, 1000, random())                                as amount,
    dateadd(minute, -seq4(), current_timestamp())::timestamp_ntz as updated_at
from table(generator(rowcount => 1000))
