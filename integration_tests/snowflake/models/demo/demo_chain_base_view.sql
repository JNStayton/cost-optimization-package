-- View chain slice (phase F2): demo_chain_base_view → demo_chain_mid_view → demo_chain_step
-- (ephemeral) → demo_chain_table. Real work, so the view probe's hash_agg(*) measures
-- a nonzero recompute time. Reads, view builds and table builds come from the fixture
-- query history.
select
    seq4()                          as id,
    uniform(1, 100, random(42))     as amount,
    md5(seq4()::varchar)            as label
from table(generator(rowcount => 20000000))
