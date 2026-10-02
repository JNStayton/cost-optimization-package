{#-
  Simulated SHOW WAREHOUSES for the multi-cluster branches (Enterprise edition). Fixture
  warehouses don't exist in the account, so the package's refresh_warehouse_config hook
  never sees them; this post-hook (dbt_project.yml) sets the values SHOW WAREHOUSES would
  return, the same way that hook's merge does, including marking a warehouse with a max
  cluster count above 1 as multi-cluster.
    - BURSTY:        auto-suspend 60, single cluster → 1.6 enable multi-cluster (bursty)
    - MCW_IDLE:      auto-suspend 60, ECONOMY, 1–3 clusters → 1.2 switch scaling policy
    - MCW_BUSY:      STANDARD, 1–3 clusters, queuing → 2.5 increase max clusters
    - MCW_OVERSIZED: STANDARD, 1–2 clusters, 10% load → 5.1 disable multi-cluster
-#}
{% macro simulate_show_warehouses() %}
  {% if execute and var('snowflake_enterprise_edition', true) %}
    merge into {{ this }} as target
    using (
        select column1 as warehouse_name, column2 as auto_suspend_seconds, column3 as scaling_policy,
               column4 as min_cluster_count, column5 as max_cluster_count
        from values
            ('FIXTURE_WH_BURSTY',        60,  'STANDARD', 1, 1),
            ('FIXTURE_WH_MCW_IDLE',      60,  'ECONOMY',  1, 3),
            ('FIXTURE_WH_MCW_BUSY',      300, 'STANDARD', 1, 3),
            ('FIXTURE_WH_MCW_OVERSIZED', 300, 'STANDARD', 1, 2)
    ) as source
    on target.warehouse_name = source.warehouse_name
    when matched then update set
        auto_suspend_seconds = source.auto_suspend_seconds,
        auto_resume          = true,
        scaling_policy       = source.scaling_policy,
        min_cluster_count    = source.min_cluster_count,
        max_cluster_count    = source.max_cluster_count,
        is_multicluster      = target.is_multicluster or source.max_cluster_count > 1,
        warehouse_category   = iff(target.is_multicluster or source.max_cluster_count > 1,
                                   'multi_cluster', target.warehouse_category)
  {% else %}
    select 1
  {% endif %}
{% endmacro %}
