{{ config(materialized='table') }}
-- View chain slice: the table at the end of the chain. Each of its builds (fixture query
-- history) recomputes both views and the ephemeral.
select * from {{ ref('demo_chain_step') }}
