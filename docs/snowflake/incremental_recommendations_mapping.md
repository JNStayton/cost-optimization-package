# Incremental Materialization Recommendations Mapping

## Design Principle

Telemetry may identify and prioritize a candidate, propose an implementation, and quantify the assumptions. The package always supplies the best template it can construct, with explicit assumptions and confidence scoring. The engineer reviews, tests, and promotes.

---

## Recommendation Architecture

### Confidence-Based Output

Every incremental candidate receives:

| Field | Purpose |
|-------|---------|
| `confidence_score` | Numeric score (0-100) reflecting how much telemetry support exists for the recommendation |
| `recommendation_status` | Derived from confidence: `actionable_review` / `investigate` / `do_not_recommend` |
| `assumptions` | Array of inferences the package made to construct the template |
| `blocking_signals` | Array of factors that reduced confidence |
| `dbt_model_config` | Proposed template (always supplied when a strategy can be proposed, regardless of confidence) |
| `next_evidence_needed` | For `investigate` status: what's missing that prevents proposing a strategy |

### Status Derivation

| Confidence range | Status | Template? | Meaning |
|-----------------|--------|-----------|---------|
| >= 60 | `actionable_review` | Yes | High confidence. Test before promoting to production. |
| 30-59 | `investigate` | Yes (with prominent "verify these" warnings) | Moderate confidence. Template is a starting point but assumptions need verification. |
| 1-29 | `investigate` | Yes (if constructible) | Low confidence. Template is speculative — significant verification required. |
| 0 | `do_not_recommend` | No (cannot construct) | Either low ROI or no strategy can be proposed (e.g., no timestamp column) |

**Key principle**: Confidence gates the LABEL, not the CONTENT. If we can construct a template, we always show it. The engineer decides whether to trust it based on the assumptions list and confidence score.

---

## Confidence Scoring Model

### Starting Score

All candidates that pass the ROI gate start at **100**.

### Deductions

| Category | Signal | Deduction | Rationale |
|----------|--------|-----------|-----------|
| **Assumptions** (inferred, not proven) | | | |
| | Proposed watermark from naming convention (not confirmed) | -10 | May not be the correct change boundary |
| | Assumed time-local (no contradicting evidence but not proven) | -10 | Window functions / rankings could invalidate |
| | Assumed append-only from INSERT-only target DML | -15 | Target DML doesn't prove source behavior |
| | Proposed lookback window (default, not validated against late arrivals) | -10 | Late arrivals could cause missed data |
| | Key inferred from naming convention (not yet probed) | -10 | May not be truly unique |
| **Blocking signals** (observed risks) | | | |
| | Key probe failed (not exact or has nulls) | -30 | Merge/delete+insert will produce incorrect results |
| | Target DELETEs observed (no reconciliation mechanism known) | -25 | Deletes won't propagate; target will retain stale rows |
| | Target UPDATEs/MERGEs observed (mutable rows without validated key) | -20 | Unknown mutation pattern |
| | Watermark does not advance across observed builds | -20 | Proposed filter column may not track new data |
| | High null rate on proposed watermark (> 5%) | -15 | Filter will miss rows |
| | Low build frequency (< 0.2 builds/day) | -10 | Savings will be small in absolute terms |
| **Bonuses** (validated signals) | | | |
| | Key probe passes exact uniqueness + zero nulls | +10 (restores deduction) | Key is validated on target |
| | Watermark advances monotonically across builds | +5 | Good evidence of usable change boundary |
| | INSERT-only DML AND no DELETEs for 30+ days | +5 | Stronger append-only evidence |

### ROI Gate (precedes confidence scoring)

| Tier | Condition | Effect |
|------|-----------|--------|
| `high` | `median_build_time_sec >= 300` AND `qualified_build_days >= 3` (across 14+ days) AND `redundancy >= 0.70` | Enters confidence scoring |
| `medium` | `median_build_time_sec >= 120` AND `redundancy >= 0.70` | Enters confidence scoring (max status = `investigate`) |
| `low` | Everything else | `do_not_recommend` — does not enter confidence scoring |

---

## Strategy Selection

Strategies are selected by **data-change semantics**, not table size. The package infers the most appropriate strategy from available telemetry.

### Strategy Inference Logic

| # | Condition (inferred from telemetry) | Strategy | Assumptions made |
|---|---|---|---|
| S.1 | Has proposed watermark + exact non-null key + no target deletes + no target updates | `merge` | "Watermark is change boundary", "data is time-local", "lookback captures late arrivals" |
| S.2 | Has proposed watermark + INSERT-only target DML + no key needed (or key confirmed) | `append` | "Source is append-only", "no late arrivals beyond lookback", "no upstream mutations" |
| S.3 | Has proposed watermark + exact key + target deletes observed | `merge` (with delete warning) | Same as S.1 + "deletes will NOT propagate — target retains deleted rows unless reconciliation is implemented" |
| S.4 | Has proposed watermark + no key candidate | `append` (weaker confidence) | "Source is append-only", "duplicates acceptable or won't occur", "no key needed" |
| S.5 | No proposed watermark candidate | Cannot propose strategy | No template possible → `investigate` or `do_not_recommend` |

### Phase 1 vs Phase 2

| Strategy | Phase 1 status | Phase 2 potential |
|----------|---------------|-------------------|
| `merge` | `actionable_review` (when confidence >= 60) | Same |
| `append` | `actionable_review` (when confidence >= 60) | Same |
| `delete+insert` | `investigate` (always — can't validate slice recomputability) | Upgradeable with measured slice selectivity |
| `microbatch` | `investigate` (always — can't validate batch independence/parent propagation) | Upgradeable with event-time validation |

---

## Observed Signals

| Signal | Source | What it tells us | How it affects confidence |
|--------|--------|-----------------|--------------------------|
| `rebuild_redundancy_rate` | First/last CTAS rows_inserted | Fraction of rebuild that's unchanged data | Gates ROI tier |
| `median_build_time_sec` | Median execution time of CTAS runs | Actual cost (robust against outliers) | Gates ROI tier |
| `builds_per_day` | COUNT of CTAS runs / days | Frequency of waste | Gates ROI tier; low freq = -10 |
| `qualified_build_days` | Distinct CTAS days in lookback | Signal reliability | Gates ROI tier |
| `target_dml_evidence` | DML types on TARGET in query_history | Risk signal (not source proof) | DELETEs = -25; UPDATEs = -20; INSERT-only = +5 |
| `candidate_filter_columns` | Timestamp/date columns by naming pattern | Proposed watermark | Enables template; absence = can't propose |
| `candidate_key_columns` | Columns matching *_id, *_key, *_sk | Proposed grain | Enables merge; absence = weaker confidence |
| `exact_key_probe` | `count(*) = count(distinct key) AND null_count = 0` | Key validity on target | Pass = +10; Fail = -30 |
| `watermark_evidence` | Profiling: null rate, advancement, recency | Watermark quality | Informs deductions (null > 5% = -15, no advancement = -20) |
| `rebuild_pressure_score` | `table_size_gb * builds_per_day` | Ranking signal | For prioritization only |

---

## Watermark Evidence (profiled for high-ROI candidates)

| Metric | What it reveals | Confidence impact |
|--------|----------------|-------------------|
| Null rate | Whether column supports filtering | > 5% nulls = -15 |
| Min / max value | Active range | Max is years old = -10 |
| Advancement across builds | Whether watermark tracks new data | No advancement = -20; advances = +5 |
| Recent-window row share | Whether most data is recent | < 20% recent = weaker watermark |
| Concentration at build time | Identifies `_loaded_at`-style columns | High concentration = likely a good watermark |

---

## Expected Outputs by Model Archetype

### dim_customers (no timestamp columns)

| Field | Value |
|-------|-------|
| roi_tier | high (100% redundancy, 2.9GB, builds > 300s) |
| confidence_score | 0 |
| recommendation_status | do_not_recommend (for incremental) |
| assumptions | [] |
| blocking_signals | `['missing_filter_column']` |
| dbt_model_config | NULL (cannot construct — no watermark) |
| next_evidence_needed | "Add a timestamp/date column (e.g., _loaded_at) to enable incremental" |
| recommended_optimization | clustering (P2 — immediately actionable from clustering domain) |

### int_order_items (has order_date, confirmed key, INSERT-only DML)

| Field | Value |
|-------|-------|
| roi_tier | high |
| confidence_score | 75 (100 - 10 watermark inferred - 10 time-local assumed - 10 lookback assumed + 10 key confirmed - 5 other minor) |
| recommendation_status | actionable_review |
| proposed_strategy | merge |
| assumptions | ["order_date is a reliable change boundary", "data is time-local (historical output does not change outside lookback)", "3-day lookback captures late-arriving records"] |
| blocking_signals | `['incoming_slice_key_not_validated']` |
| dbt_model_config | Full merge template with assumptions as comments |

### Model with target DELETEs observed

| Field | Value |
|-------|-------|
| roi_tier | high |
| confidence_score | 45 (100 - 10 watermark - 10 time-local - 10 lookback - 25 deletes) |
| recommendation_status | investigate |
| assumptions | ["order_date is a reliable change boundary", "data is time-local"] |
| blocking_signals | `['deletes_observed_without_reconciliation']` |
| dbt_model_config | Merge template WITH prominent warning: "Target deletes observed — this template will NOT propagate source deletions. Implement a reconciliation mechanism or accept stale rows." |

### Model with no key candidate but has timestamp

| Field | Value |
|-------|-------|
| roi_tier | high |
| confidence_score | 55 (100 - 10 watermark - 10 time-local - 10 lookback - 15 append assumed) |
| recommendation_status | investigate |
| assumptions | ["source is append-only (INSERT-only observed)", "no late arrivals beyond lookback", "duplicates won't occur"] |
| blocking_signals | `['append_only_not_proven']` |
| dbt_model_config | Append template with assumptions as comments |

---

## Effort Category

All incremental recommendations use effort_category = `actionable_review`. This signals:
- Not a simple config flip (like auto-suspend changes)
- Requires human review of assumptions
- Requires testing in dev/staging before production
- May need minor adjustments (85-100% correct as-supplied)

---

## Cross-Domain Interactions

| Signal | How it connects | Priority interaction |
|--------|----------------|---------------------|
| Spillage on same model | Full rebuild overflows memory | Clustering P1 if co-occurs; incremental (if actionable_review) reduces working set |
| Clustering candidate | Same table needs both | Clustering immediately actionable (config_change); incremental is actionable_review (higher effort) |
| Expensive query | Cost driven by rebuild waste | Incremental addresses root cause; co-signal makes it more urgent |
| No timestamp (dim table) | Can't propose incremental | Clustering is the primary optimization; incremental is investigate/do_not_recommend |

---

## Removed from Scope

| Item | Reason |
|------|--------|
| `insert_overwrite` | Excluded from v1 automated recommendations. The package lacks partition-replacement semantics and workload evidence needed for a safe recommendation. |
| Automatic append without evidence | Append without strong append-only evidence risks silent data loss |
| Size-based strategy selection | Table size does not determine data-change semantics |
| 95% approximate uniqueness | Replaced by exact probe |
| Meta tag declarations | Removed — package infers everything from telemetry. Assumptions are listed explicitly. |

---

## Phase 2 Enhancements

| Enhancement | Signal needed | Impact |
|---|---|---|
| Late-arrival distribution | Ingestion timestamp vs event timestamp comparison | Validates lookback quantitatively; improves confidence |
| Per-run redundancy median | Finer-grained per-build tracking | More robust ROI |
| Incoming-slice key validation | Dry-run of proposed incremental query | Validates key on actual change set |
| delete+insert → actionable_review | Measured slice selectivity + validated recomputability | Upgrades from investigate |
| microbatch → actionable_review | Parent event-time propagation + batch independence | Upgrades from investigate |
| Composite key detection | GROUP BY patterns in downstream queries | Multi-column grain identification |
| Source contract inference | EL tool metadata (Fivetran, Airbyte markers) | Auto-strengthens confidence without manual declaration |
