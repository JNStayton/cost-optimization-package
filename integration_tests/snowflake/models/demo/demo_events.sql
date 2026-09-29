{{ config(materialized='table') }}
-- Demo fact table for the clustering tests. Columns span a range of cardinalities so
-- the package's clustering-key scoring has something to rank:
--   region (5 values), event_date (30), customer_id (5,000), is_test (boolean).
-- Query activity comes from models/account_usage_fixtures/fixture_query_history.sql,
-- which runs real filtered SELECTs against this table.
select
    seq4()                                             as event_id,
    dateadd(day, -(seq4() % 30), current_date())       as event_date,
    seq4() % 5000                                      as customer_id,
    decode(seq4() % 5, 0, 'NA', 1, 'EU', 2, 'APAC', 3, 'LATAM', 'MEA') as region,
    (seq4() % 50 = 0)                                  as is_test,
    uniform(1, 1000, seq4())                           as amount
from table(generator(rowcount => 200000))
