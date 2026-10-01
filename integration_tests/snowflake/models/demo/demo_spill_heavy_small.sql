{{ config(materialized='table') }}
-- Spillage cases (Enterprise edition): the fixture query history gives this table's builds
-- their spill. See models/account_usage_fixtures/fixture_query_history.sql.
select seq4() as id from table(generator(rowcount => 1000))
