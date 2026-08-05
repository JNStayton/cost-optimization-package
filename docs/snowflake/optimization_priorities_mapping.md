# Optimization Priority Mapping

## v1 vs v2 Scope

| Phase | Requires | What it enables |
|-------|----------|----------------|
| **v1 (stateless)** | Current run's signals only | Priority assignment, co-signal deferral, sql_refactor non-blocking rule |
| **v2 (historical)** | Incremental facts + lifecycle model | Resolution detection, temporal promotion (P3→P1 validated need), observation windows |

Rules marked **(v2)** are documented for completeness but will not be implemented until incremental facts are in place. The v1 implementation uses stateless rules only — no run-over-run comparison is needed.

### v1 Limitations

1. **No resolution detection** — cannot determine if a previously recommended optimization was applied. Promotion relies solely on co-signal absence in the current run.
2. **No temporal anchoring** — cannot distinguish "signal disappeared because fix was applied" from "signal disappeared because data changed."
3. **Promotion ceiling** — v1 max promotion is P3→P2. The P3→P1 "validated need" path (symptom persists after fix applied) requires v2's observation window.
4. **Shared warehouse attribution** — spillage deferral checks only the specific spilling models for clustering/materialization candidates (table_fqn join), not all models on the warehouse.

---

## Priority Tiers

| Tier | Label | Meaning | Gold view behavior |
|------|-------|---------|-------------------|
| P1 | Actionable now | Safe to apply immediately, no prerequisites, or promoted because conditions met | top_recommendations, backlog (filtered), cross_domain_insights |
| P2 | Root cause fix | Addresses structural inefficiency; should be attempted before P3 config changes | Domain views + backlog |
| P3 | Deferred config | Infrastructure change gated behind P2 resolution | Domain views only; promoted to P2 when stateless conditions met |
| P4 | Monitor | Trending signal not yet at action threshold | Domain views only |

### Configurable Thresholds

All dollar-based promotion thresholds are dbt project variables:

```yaml
vars:
  min_savings_threshold_annual: 100   # USD — minimum savings for P2→P1 promotion
  credit_rate_usd: 2                  # USD per credit
  priority_observation_days: 7        # v2 only: days to wait before validated promotion
```

---

## Priority Rules (v1 — Stateless)

Rules are evaluated in order. A signal's final priority is determined by the first matching rule.

### Rule 1: Co-Signal Deferral (evaluated first)

If a P3 warehouse config signal exists AND a P2 model-level signal exists for models that are causing the symptom:
- P3 stays P3 (deferred)
- The P2 model fix is the correct first action

**Blocking criterion**: Any P2 signal with `effort_category NOT IN ('sql_refactor', 'monitoring')` blocks P3 promotion for the same entity.

**Join grain**: The deferral check joins spillage/overload stats (per model/table_fqn) to clustering/materialization candidates (per table_fqn). Only the specific models contributing to the warehouse symptom are checked — not all models on the warehouse.

If NO blocking P2 model-level signal exists for the affected models:
- P3 is **promoted to P2** (no root-cause fix available)

### Rule 2: SQL Refactor Non-Blocking (evaluated second, on remaining P3 signals)

Applies only to P3 signals that survived Rule 1's deferral check.

If the only remaining P2 signals for an entity have `effort_category = 'sql_refactor'`:
- P3 config changes for that entity are **promoted to P2**
- Both the sql_refactor AND the config change surface as active
- Rationale: teams can apply quick config fix now, work on SQL optimization in parallel

### Rule 3: Savings-Based Promotion (P2 → P1)

A P2 signal is promoted to P1 when `estimated_annual_savings_usd >= var('min_savings_threshold_annual', 100)`.

### Rule 4: v2 Only — Resolution-Based Promotion

*Deferred to v2 (requires incremental facts + lifecycle model).*

When a P2 signal was present in a previous run and is absent in the current run:
- Any P3 signal for the same entity that was waiting on it is promoted to P2
- If the warehouse-level SYMPTOM persists (e.g., spillage still above threshold) despite the P2 fix being resolved, the P3 becomes P1 — but only after `var('priority_observation_days', 7)` days post-resolution to avoid false positives (e.g., reclustering hasn't finished yet)

---

## Full Signal Inventory

### Warehouse Domain — Always-Actionable (Base: P1)

Low-risk config changes. Safe regardless of model state.

| signal_id | effort_category | Recommendation | Condition to fire | P1 promotion | Notes |
|-----------|----------------|---------------|-------------------|-------------|-------|
| `idle_reduce_auto_suspend` | config_change | Reduce auto-suspend to 60s | idle_pct > 20% AND auto_suspend > 60s | Always P1 | Saves credits immediately |
| `idle_switch_scaling_policy` | config_change | Switch ECONOMY to STANDARD | idle_pct > 20% AND auto_suspend <= 60s AND MCW ECONOMY | Always P1 | ECONOMY keeps clusters hot |
| `idle_reduce_max_clusters` | config_change | Reduce max cluster count | idle_pct > 20% AND STANDARD scaling AND max > 2 | Always P1 | Reduces over-provisioning |
| `idle_reduce_min_clusters` | config_change | Set min clusters to 1 | idle_pct > 20% AND min > 1 | Always P1 | Allows full scale-down |
| `idle_enable_mcw_bursty` | config_change | Enable MCW for bursty workload | idle_pct > 20% AND load > 80% | Always P1 | Condition validates bursty pattern (high idle + high load = burst) |
| `provisioning_enable_auto_resume` | config_change | Enable auto-resume | provisioning_ms > 2000 AND auto_resume = false | Always P1 | Removes manual bottleneck |
| `provisioning_increase_suspend` | config_change | Increase auto-suspend to 60s | provisioning_ms > 2000 AND auto_suspend < 60s | Always P1 | Reduces cold-start frequency |
| `provisioning_increase_suspend_300` | config_change | Increase auto-suspend to 300s | provisioning_ms > 2000 AND 60 <= auto_suspend < 300 | Always P1 | Keeps warehouse warm between bursts |
| `provisioning_warm_cluster` | config_change | Set min clusters to 1 (warm) | provisioning_ms > 2000 AND MCW AND min=0 | Always P1 | Simple config, immediate effect |
| `overload_switch_scaling_policy` | config_change | Switch ECONOMY to STANDARD (queuing despite MCW) | overload > 10% AND MCW AND ECONOMY | Always P1 | ECONOMY delays cluster spin-up |

### Warehouse Domain — Conditional Config (Base: P3, promotable)

Wait for model-level fixes unless no blocking model signal exists or sql_refactor is the only remaining signal.

| signal_id | effort_category | Recommendation | Condition to fire | Promotion condition (v1) | Notes |
|-----------|----------------|---------------|-------------------|--------------------------|-------|
| `overload_enable_mcw` | config_change | Enable multi-cluster | overload > 10% AND not MCW AND Enterprise | P2 if no blocking P2 signal for affected models | Queuing may resolve with query efficiency |
| `overload_scale_up_standard` | config_change | Scale up (Standard edition) | overload > 10% AND Standard AND not smallest | P2 if no blocking P2 signal | Standard has no MCW option |
| `overload_increase_clusters` | config_change | Increase max clusters | overload > 10% AND MCW STANDARD AND max < 10 | P2 if no blocking P2 signal | Cluster ceiling not yet reached |
| `overload_scale_up_large_mcw` | config_change | Scale up (MCW at ceiling) | overload > 10% AND max >= 10 | P2 if no blocking P2 signal | Last resort — cluster ceiling reached |
| `spillage_scale_up` | config_change | Scale up warehouse (heavy spillage) | heavy local spillage AND is_smallest_size = false | P3 if clustering signal exists for the SPILLING MODELS; P2 if no clustering signal for them | Key cross-domain deferral example |
| `oversized_scale_down` | config_change | Scale down warehouse | load < 50% AND exec < 0.5s AND no queuing AND is_smallest_size = false | P2 if no `convert_to_incremental` or `materialize_as_table` signal for models on this warehouse | Don't scale down if model changes will reduce load |
| `oversized_disable_mcw` | config_change | Disable MCW (oversized) | load < 30% AND MCW AND no queuing | Same logic as scale_down | |

### Warehouse Domain — Architecture (Base: P3)

Requires workload planning — not simple config flips.

| signal_id | effort_category | Recommendation | Condition to fire | Promotion condition (v1) | Notes |
|-----------|----------------|---------------|-------------------|--------------------------|-------|
| `overload_at_max_standard` | architecture | Split workloads across warehouses | overload AND Standard AND is_smallest_size | Not promotable in v1 | Requires workload migration |
| `idle_consolidate_standard` | architecture | Consolidate workloads (Standard) | idle > 20% AND all config levers exhausted AND Standard | Not promotable in v1 | Requires workload planning |
| `idle_consolidate_underloaded` | architecture | Consolidate underloaded warehouse | idle > 20% AND load < 50% AND all config levers exhausted | Not promotable in v1 | Requires workload migration |

### Warehouse Domain — Monitor (Base: P4)

| signal_id | effort_category | Recommendation | Condition to fire | Notes |
|-----------|----------------|---------------|-------------------|-------|
| `spillage_moderate_worsening` | monitoring | Monitor — moderate spillage trending worse | moderate spill AND trend worsening | Early warning; stays P4 in v1 |
| `spillage_moderate_stable` | monitoring | Monitor — moderate spillage stable | moderate spill AND trend stable | Informational only |
| `expensive_query_monitor` | monitoring | Monitor — recurring credit consumption | cost trend worsening | Informational; does not block P3 promotion |

### Warehouse Domain — Other (Base: P2)

| signal_id | effort_category | Recommendation | Condition to fire | Promotion condition (v1) | Notes |
|-----------|----------------|---------------|-------------------|--------------------------|-------|
| `provisioning_gen2` | config_change | Migrate to Gen2 | provisioning > 2000ms AND auto_suspend >= 300 AND not gen2 | P1 if savings >= threshold | Infrastructure upgrade |

### Warehouse Domain — Suppression Reasons (not signals)

These explain why a recommendation was NOT made. They do not appear as rows in `int_all_recommendations`. They are metadata on the warehouse config fact for audit/explainability.

| suppression_id | When it applies | Explanation |
|---------------|-----------------|-------------|
| `oversized_at_minimum` | Oversized metrics detected AND is_smallest_size = true | Cannot scale down further — consider workload consolidation |
| `oversized_write_heavy` | Oversized metrics detected AND dml_ratio > 20% | Write-heavy workloads need larger sizes for COPY/INSERT performance |
| `stable` | No symptom thresholds crossed | Healthy — no configuration changes needed |

---

### Model Domain — Materialization (Base: P2)

| signal_id | effort_category | Recommendation | Condition to fire | Promotion condition (v1) | Notes |
|-----------|----------------|---------------|-------------------|--------------------------|-------|
| `materialize_as_table` | config_change | Materialize as TABLE | View with high select count, score above threshold | P1 if savings >= threshold | Reduces downstream recomputation |
| `convert_to_incremental` | actionable_review | Convert to incremental (strong/good) | High rebuild redundancy + confidence_score >= 60 | P1 if savings >= threshold AND recommendation_status = 'actionable_review' | Only actionable_review recs participate in cross-domain deferral |
| `apply_incremental_append` | actionable_review | Apply incremental config: append | Strategy = append, filter column identified, confidence >= 60 | P1 when parent `convert_to_incremental` is P1 (savings >= threshold) | Template includes assumptions to verify |
| `apply_incremental_merge` | actionable_review | Apply incremental config: merge | Strategy = merge, key probe pending/passed, confidence >= 60 | P1 when parent `convert_to_incremental` is P1 (savings >= threshold) | Template includes assumptions to verify |
| `apply_incremental_delete_insert` | actionable_review | Apply incremental config: delete+insert | Investigate only in v1 (requires slice recomputability) | Not promotable in v1 | Phase 2: upgradeable with validated slice selectivity |

### Model Domain — Clustering (Base: P2)

| signal_id | effort_category | Recommendation | Condition to fire | Promotion condition (v1) | Notes |
|-----------|----------------|---------------|-------------------|--------------------------|-------|
| `add_clustering_key_strong` | config_change | Add clustering key (strong signal) | Strong tier, filter proportion >= 33% | P1 if spillage co-occurs on same model | Clustering directly addresses scan width → spillage |
| `add_clustering_key_good` | config_change | Add clustering key (good signal) | Good tier, passes gating | P1 if spillage co-occurs | Same rationale, slightly weaker signal |

### Model Domain — SQL Refactor (Base: P2, non-blocking)

| signal_id | effort_category | Recommendation | Condition to fire | Promotion condition (v1) | Notes |
|-----------|----------------|---------------|-------------------|--------------------------|-------|
| `expensive_query_actionable` | sql_refactor | Refactor expensive recurring query | High cost, attributable to specific model | P1 if savings >= threshold | Explicitly non-blocking: does NOT prevent P3→P2 promotion for warehouse config |

---

## Interaction Matrix (v1 — Stateless)

How signals from different domains interact on the same entity:

| Model state | Warehouse signal | Final priority | Join grain | Rationale |
|-------------|-----------------|---------------|-----------|-----------|
| Clustering candidate exists for spilling model | spillage_scale_up | P3 (deferred) | table_fqn match: spillage fact → clustering candidates | Cluster first, then evaluate |
| No clustering candidate for spilling model | spillage_scale_up | P2 (promoted) | Same join, no match found | No root-cause fix available |
| Incremental candidate exists (actionable_review) for queuing model | overload_enable_mcw | P3 (deferred) | model warehouse_name → incremental candidates table_fqn | Reduce rebuild waste first |
| Incremental candidate exists (investigate only) for queuing model | overload_enable_mcw | NOT deferred | investigate recs don't block warehouse config | Low-confidence signals don't gate infrastructure |
| Only sql_refactor for affected models | Any P3 warehouse config | P2 (promoted) | Rule 2 applies after Rule 1 | sql_refactor is non-blocking |
| No model-level signals exist | Any warehouse config | Base tier applies | No P2 signals found for models on warehouse | Nothing to defer behind |
| convert_to_incremental (actionable_review) exists | oversized_scale_down | P3 (deferred) | Model on this warehouse has validated incremental signal | Load may decrease once applied — premature to scale down |
| convert_to_incremental (investigate only) exists | oversized_scale_down | NOT deferred | investigate recs don't block | Low-confidence signal doesn't gate |

### Mutually Exclusive Symptoms

The classified CTE in `fct_snowflake__warehouse_config_recommendations` uses first-match-wins priority:
1. idle_credit_consumption (idle_pct > 20%)
2. query_overload (overload_ratio > 10% OR overload_sec > 0.5)
3. queued_provisioning (provisioning_ms > 2000)
4. oversized (load < 50% AND exec < 0.5s AND no queuing)
5. healthy (none of the above)

A warehouse cannot simultaneously be "oversized" and have "query_overload" — overload is checked first and would win. If contradictory metrics appear (low load overall but bursty queuing), the bursty pattern is captured by `idle_enable_mcw_bursty` instead.

Note: `idle_enable_mcw_bursty` is a sub-recommendation within the `idle_credit_consumption` symptom class — it fires when idle_pct > 20% (classified as idle first) AND load > 80% (bursty pattern detected). It is not a separate symptom; it is a specific branch within idle classification where the fix is MCW rather than suspend/scaling changes.

---

## Gold View Behavior

| View | Shows | Priority filter | Order |
|------|-------|----------------|-------|
| `vw_snowflake__warehouse_optimizations` | 1 row per (warehouse, signal category); aggregated affected_models for model-level signals | None — shows all tiers | warehouse, hierarchy_rank, priority_tier |
| `vw_snowflake__dbt_model_optimizations` | ACTIONABLE model signals only (excludes investigate/do_not_recommend) | Actionable only | model_name, priority_tier |
| `vw_snowflake__top_expensive_queries` | Top 10 expensive queries with co-signal enrichment | None — cost-ranked | cost desc |
| `vw_snowflake__top_recommendations` | Highest-impact across all domains | P1 + P2 per entity | priority_tier, savings desc |
| `vw_snowflake__optimization_backlog` | Full inventory — ALL tiers (users filter by priority_tier) | None — shows all | priority_tier, savings desc |
| `vw_snowflake__cross_domain_insights` | Multi-signal correlation | Multi-signal tables only | signal_count desc |

---

## Implementation Order

1. Create this mapping doc (design reference)
2. Add `priority_tier` + `signal_id` columns to `int_snowflake__all_recommendations` with v1 stateless logic
3. Fix remaining bugs (scope filter project-aware, append unique key downgrade path)
4. Update gold views — warehouse shows all signals, model shows all, filtered views respect tiers
5. Convert facts to incremental — 180-day configurable retention (enables v2)
6. Build optimization lifecycle model — resolution detection, temporal promotion (v2)
