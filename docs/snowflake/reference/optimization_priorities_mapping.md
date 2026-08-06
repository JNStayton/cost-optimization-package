# Optimization Priority Mapping

## Design Principle

Every entity (model or warehouse) receives a **per-entity relative priority**. Priority tiers are not fixed global labels — they are computed per-entity based on what other signals exist for that same entity.

- **P1** = do this first (highest in the optimization hierarchy for this model/warehouse)
- **P2** = do this second (after P1 is applied)
- **P3+** = deferred (waiting for higher-priority fixes to resolve the symptom)

Priority cascades naturally: when you apply a P1 fix (e.g., convert to incremental) and rebuild, the signal disappears and P2 promotes to P1 automatically.

---

## How Priority Is Computed

Priority is a `row_number()` window function partitioned by entity, ordered by three dimensions:

```sql
row_number() over (
    partition by coalesce(node_id, entity_name)
    order by
        -- 1. Backlog status (actionable items first)
        case backlog_status
            when 'actionable' then 1
            when 'monitor' then 2
            else 3
        end,
        -- 2. Hierarchy rank (optimization decision order)
        case [hierarchy_rank by domain] end,
        -- 3. Savings (break ties within same rank)
        estimated_annual_savings_usd desc nulls last
) as priority_tier
```

### Backlog Status (first sort)

Every signal gets a `backlog_status` derived from its domain logic:

| Status | Meaning | Source |
|--------|---------|--------|
| `actionable` | Ready to apply/review — has template or DDL | Warehouse config recs, actionable_review incremental, clustering candidates, materialization candidates |
| `monitor` | Trending signal, not yet actionable | Moderate spillage, expensive queries, investigate-status incremental |
| `stable` | Below threshold or do_not_recommend | Low ROI incremental, healthy warehouses |

Backlog status as first sort ensures investigate items never outrank actionable ones for the same entity.

### Hierarchy Rank (second sort)

Fixed position in the optimization decision hierarchy — determines which type of optimization should be attempted first:

| Rank | Domain | Rationale |
|------|--------|-----------|
| 1 | Always-safe warehouse config (idle, provisioning) | Zero risk, immediate savings, never wrong |
| 2 | Materialization / incremental | Addresses root cause (redundant work) — most impactful structural fix |
| 3 | Clustering | Addresses scan efficiency — fixes symptoms after materialization is correct |
| 4 | Conditional warehouse config (scale-up, MCW) | Infrastructure change that may become unnecessary after model fixes |
| 5 | Monitor signals (expensive queries, moderate spillage) | Informational — no action template |
| 6 | AI/Cortex | Different optimization surface |

### Cascade Behavior

When an optimization is applied:
1. Its signal vanishes from the next run (the condition that fired it no longer exists)
2. Lower-priority signals for the same entity automatically promote (their `row_number()` decreases)
3. No manual re-prioritization needed — the window function handles it

Example: Model has clustering (P1) + incremental (P2) + spillage_scale_up (P3). You add the clustering key. Next run: clustering signal gone, incremental becomes P1, spillage becomes P2.

---

## Scope and Filtering

Signals that should never surface are excluded before priority assignment:

- `do_not_recommend` incremental recs (low ROI)
- `apply_incremental_*` signals where `incremental_strategy IS NULL` (no strategy could be inferred)
- Signals outside the project scope (controlled by `dbt_monitored_projects` variable)

The `int_snowflake__all_recommendations` table applies these filters before computing priority_tier.

---

## Full Signal Inventory

### Warehouse Domain — Always-Actionable (Hierarchy Rank 1)

Low-risk config changes. Safe regardless of model state.

| signal_id | effort_category | Recommendation | Condition to fire | Notes |
|-----------|----------------|---------------|-------------------|-------|
| `idle_reduce_auto_suspend` | config_change | Reduce auto-suspend to 60s | idle_pct > 20% AND auto_suspend > 60s | Saves credits immediately |
| `idle_switch_scaling_policy` | config_change | Switch ECONOMY to STANDARD | idle_pct > 20% AND auto_suspend <= 60s AND MCW ECONOMY | ECONOMY keeps clusters hot |
| `idle_reduce_max_clusters` | config_change | Reduce max cluster count | idle_pct > 20% AND STANDARD scaling AND max > 2 | Reduces over-provisioning |
| `idle_reduce_min_clusters` | config_change | Set min clusters to 1 | idle_pct > 20% AND min > 1 | Allows full scale-down |
| `idle_enable_mcw_bursty` | config_change | Enable MCW for bursty workload | idle_pct > 20% AND load > 80% | Validates bursty pattern (high idle + high load = burst) |
| `provisioning_enable_auto_resume` | config_change | Enable auto-resume | provisioning_ms > 2000 AND auto_resume = false | Removes manual bottleneck |
| `provisioning_increase_suspend` | config_change | Increase auto-suspend to 60s | provisioning_ms > 2000 AND auto_suspend < 60s | Reduces cold-start frequency |
| `provisioning_increase_suspend_300` | config_change | Increase auto-suspend to 300s | provisioning_ms > 2000 AND 60 <= auto_suspend < 300 | Keeps warehouse warm between bursts |
| `provisioning_warm_cluster` | config_change | Set min clusters to 1 (warm) | provisioning_ms > 2000 AND MCW AND min=0 | Simple config, immediate effect |
| `overload_switch_scaling_policy` | config_change | Switch ECONOMY to STANDARD (queuing despite MCW) | overload > 10% AND MCW AND ECONOMY | ECONOMY delays cluster spin-up |

### Warehouse Domain — Conditional Config (Hierarchy Rank 4)

Wait for model-level fixes unless no blocking model signal exists.

| signal_id | effort_category | Recommendation | Condition to fire | Deferral logic | Notes |
|-----------|----------------|---------------|-------------------|----------------|-------|
| `overload_enable_mcw` | config_change | Enable multi-cluster | overload > 10% AND not MCW AND Enterprise | Deferred if actionable incremental/materialization exists for affected models | Queuing may resolve with model efficiency |
| `overload_scale_up_standard` | config_change | Scale up (Standard edition) | overload > 10% AND Standard AND not smallest | Same deferral logic | Standard has no MCW option |
| `overload_increase_clusters` | config_change | Increase max clusters | overload > 10% AND MCW STANDARD AND max < 10 | Same deferral logic | Cluster ceiling not yet reached |
| `overload_scale_up_large_mcw` | config_change | Scale up (MCW at ceiling) | overload > 10% AND max >= 10 | Same deferral logic | Last resort — cluster ceiling reached |
| `spillage_scale_up` | config_change | Scale up warehouse (heavy spillage) | Aggregate spillage > threshold (configurable) | Deferred if clustering/incremental signal exists for spilling models | Key cross-domain deferral example |
| `oversized_scale_down` | config_change | Scale down warehouse | load < 50% AND exec < 0.5s AND no queuing AND not smallest | Deferred if incremental/materialization signal exists for models on warehouse | Don't scale down if model changes will reduce load |
| `oversized_disable_mcw` | config_change | Disable MCW (oversized) | load < 30% AND MCW AND no queuing | Same logic as scale_down | |

### Warehouse Domain — Architecture (Hierarchy Rank 4)

Requires workload planning — not simple config flips.

| signal_id | effort_category | Recommendation | Condition to fire | Notes |
|-----------|----------------|---------------|-------------------|-------|
| `overload_at_max_standard` | architecture | Split workloads across warehouses | overload AND Standard AND is_smallest_size | Requires workload migration |
| `idle_consolidate_standard` | architecture | Consolidate workloads (Standard) | idle > 20% AND all config levers exhausted AND Standard | Requires workload planning |
| `idle_consolidate_underloaded` | architecture | Consolidate underloaded warehouse | idle > 20% AND load < 50% AND all config levers exhausted | Requires workload migration |

### Warehouse Domain — Monitor (Hierarchy Rank 5)

| signal_id | effort_category | Recommendation | Condition to fire | Notes |
|-----------|----------------|---------------|-------------------|-------|
| `spillage_moderate_worsening` | monitoring | Monitor — moderate spillage trending worse | moderate spill AND trend worsening | Early warning |
| `spillage_moderate_stable` | monitoring | Monitor — moderate spillage stable | moderate spill AND trend stable | Informational only |
| `expensive_query_monitor` | monitoring | Monitor — recurring credit consumption | cost trend worsening | Informational; does not block promotion |

### Warehouse Domain — Other (Hierarchy Rank 1)

| signal_id | effort_category | Recommendation | Condition to fire | Notes |
|-----------|----------------|---------------|-------------------|-------|
| `provisioning_gen2` | config_change | Migrate to Gen2 | provisioning > 2000ms AND auto_suspend >= 300 AND not gen2 | Infrastructure upgrade |

### Warehouse Domain — Suppression Reasons (not signals)

These explain why a recommendation was NOT made. They do not appear as rows in `int_all_recommendations`.

| suppression_id | When it applies | Explanation |
|---------------|-----------------|-------------|
| `oversized_at_minimum` | Oversized metrics detected AND is_smallest_size = true | Cannot scale down further |
| `oversized_write_heavy` | Oversized metrics detected AND dml_ratio > 20% | Write-heavy workloads need larger sizes |
| `stable` | No symptom thresholds crossed | Healthy — no changes needed |

---

### Model Domain — Materialization (Hierarchy Rank 2)

| signal_id | effort_category | Recommendation | Condition to fire | Notes |
|-----------|----------------|---------------|-------------------|-------|
| `materialize_as_table` | config_change | Materialize as TABLE | View with high select count, score above threshold | Reduces downstream recomputation |
| `convert_to_incremental` | actionable_review | Convert to incremental (strong/good) | High rebuild redundancy + confidence >= 60 | Only actionable_review recs block warehouse deferral |
| `apply_incremental_append` | actionable_review | Apply incremental config: append | Strategy = append, confidence >= 60 | Template includes assumptions to verify |
| `apply_incremental_merge` | actionable_review | Apply incremental config: merge | Strategy = merge, key validated, confidence >= 60 | Template includes assumptions to verify |

### Model Domain — Clustering (Hierarchy Rank 3)

| signal_id | effort_category | Recommendation | Condition to fire | Notes |
|-----------|----------------|---------------|-------------------|-------|
| `add_clustering_key_strong` | config_change | Add clustering key (strong signal) | Strong tier, filter proportion >= 33% | Directly addresses scan width |
| `add_clustering_key_good` | config_change | Add clustering key (good signal) | Good tier, passes gating | Same rationale, slightly weaker signal |
| `add_clustering_key_evaluate` | config_change | Evaluate clustering key | Candidate tier, needs key analysis | Recommend running `suggest_clustering_keys` macro |

### Model Domain — SQL Refactor (Hierarchy Rank 5, non-blocking)

| signal_id | effort_category | Recommendation | Condition to fire | Notes |
|-----------|----------------|---------------|-------------------|-------|
| `expensive_query_actionable` | sql_refactor | Refactor expensive recurring query | High cost, attributable to specific model | Non-blocking: does NOT defer warehouse config |

---

## Interaction Matrix

How signals from different domains interact on the same entity:

| Model state | Warehouse signal | Effect | Rationale |
|-------------|-----------------|--------|-----------|
| Clustering candidate exists for spilling model | spillage_scale_up | Deferred (higher priority_tier) | Cluster first, then evaluate |
| No clustering candidate for spilling model | spillage_scale_up | Not deferred | No root-cause fix available |
| Incremental candidate (actionable_review) for queuing model | overload_enable_mcw | Deferred | Reduce rebuild waste first |
| Incremental candidate (investigate only) for queuing model | overload_enable_mcw | NOT deferred | investigate recs don't block infrastructure |
| Only sql_refactor for affected models | Any conditional warehouse config | NOT deferred | sql_refactor is non-blocking |
| No model-level signals exist | Any warehouse config | Not deferred | Nothing to defer behind |
| convert_to_incremental (actionable_review) exists | oversized_scale_down | Deferred | Load may decrease once applied |
| convert_to_incremental (investigate only) exists | oversized_scale_down | NOT deferred | Low-confidence doesn't gate |

### Mutually Exclusive Symptoms

The classified CTE in `fct_snowflake__warehouse_config_recommendations` uses first-match-wins priority:
1. idle_credit_consumption (idle_pct > 20%)
2. query_overload (overload_ratio > 10% OR overload_sec > 0.5)
3. queued_provisioning (provisioning_ms > 2000)
4. oversized (load < 50% AND exec < 0.5s AND no queuing)
5. healthy (none of the above)

A warehouse cannot simultaneously be "oversized" and have "query_overload" — overload is checked first.

---

## Gold View Behavior

| View | Shows | Priority filter | Order |
|------|-------|----------------|-------|
| `vw_snowflake__warehouse_optimizations` | 1 row per (warehouse, signal category); aggregated affected_models | None — all tiers | warehouse, priority_tier |
| `vw_snowflake__dbt_model_optimizations` | ACTIONABLE model signals only (backlog_status = 'actionable') | Actionable only | model_name, priority_tier |
| `vw_snowflake__top_expensive_queries` | Top 10 expensive queries with co-signal enrichment | Cost-ranked | cost desc |
| `vw_snowflake__top_recommendations` | Highest-impact across all domains, P1+P2 per entity | P1 + P2 | priority_tier, savings desc |
| `vw_snowflake__optimization_backlog` | Full inventory — ALL tiers (users filter by priority_tier) | None — all | priority_tier, savings desc |
| `vw_snowflake__cross_domain_insights` | Multi-signal correlation (2+ domains per model) | Actionable + monitor | signal_count desc |
| `vw_snowflake__top_spillage_models` | Top spilling models with dbt Cloud traceability | N/A — metric-ranked | spillage desc |
| `vw_snowflake__top_queried_models` | Top 25 most-queried models by SELECT consumption | N/A — metric-ranked | query count desc |

---

## Future: Resolution Detection (v2)

Requires incremental facts with retention window. Not yet implemented.

When a signal was present in a previous run and is absent in the current run:
- Determine if the signal disappeared because a fix was applied vs data changed
- If the warehouse-level symptom persists despite the model fix being resolved, promote the deferred warehouse config signal after an observation window

This enables validated temporal promotion that the current stateless system cannot provide.
