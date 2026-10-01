{{ config(materialized='ephemeral') }}
-- View chain slice: an ephemeral between the views and the table. Never a candidate.
select bucket, amount, events
from {{ ref('demo_chain_mid_view') }}
where events > 0
