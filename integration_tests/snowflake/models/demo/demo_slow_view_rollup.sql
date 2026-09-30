{{ config(materialized='table') }}
-- Materialization slice: a table built directly from demo_slow_view, so the view feeds a
-- table. Each of its builds (fixture query history) re-runs the view's query, which counts
-- toward the view's cost and the savings from materializing it.
select * from {{ ref('demo_slow_view') }}
