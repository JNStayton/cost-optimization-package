{{ config(materialized='table') }}
-- Clustering slice: a child TABLE of demo_events. The operator-evidence hook also analyzes
-- queries on a candidate's children. Filters on this table never reach demo_events (it's a
-- table, so nothing is scanned through), so they must not count as evidence for clustering
-- demo_events. Named daily_demo_events, not demo_events_daily: Standard edition finds a
-- table's reads by checking whether the query text contains its full name, and
-- ...DEMO_EVENTS_DAILY contains ...DEMO_EVENTS.
select event_date, region, count(*) as events
from {{ ref('demo_events') }}
group by event_date, region
