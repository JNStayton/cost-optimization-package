-- View chain slice: the second view, nearer the table. It inlines demo_chain_base_view.
select
    id % 100000         as bucket,
    sum(amount)         as amount,
    count(*)            as events,
    max(label)          as last_label
from {{ ref('demo_chain_base_view') }}
group by 1
