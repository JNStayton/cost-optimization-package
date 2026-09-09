# Model Run Summary

This document covers `fct_databricks__model_run_summary`, which aggregates dbt model execution history to identify your slowest, most expensive models and surface performance trends over time.

## Pipeline Overview

```
Staging                              Intermediate                               Mart
───────                              ────────────                               ────
stg_databricks__query_history    ──► int_databricks__dbt_model_run_history  ──► fct_databricks__
  (system.query.history)                                                          model_run_summary
                                  int_dbt__relations
```

This model is a direct extension of the incremental model candidates pipeline. It reuses `int_databricks__dbt_model_run_history`, which extracts dbt model runs from query history using the JSON comment dbt prepends to every query it executes.

---

## What this model answers

- **Which dbt models are the most expensive to run?** Ranked by bytes scanned or total execution time.
- **Which models run most frequently?** High-frequency models compound their cost — a 10-second model that runs 100 times a day costs more than a 5-minute model that runs once.
- **Are any models getting slower over time?** The `performance_trend` column compares the first half of the lookback window against the second half to detect regressions.
- **What does my project cost to run each month?** `estimated_monthly_bytes_scanned_gb` extrapolates from the lookback window to give a monthly projection.

---

## Project Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `model_run_summary_lookback_days` | `7` | Number of days of run history to analyze. The trend calculation splits this window in half — first half vs. second half. Increase to 14 or 30 for more stable trend signals. |

### Example configuration

```yaml
# dbt_project.yml
vars:
  model_run_summary_lookback_days: 14
```

---

## Output Columns

| Column | Description |
|--------|-------------|
| `snapshot_date` | Date of the snapshot (one per day) |
| `dbt_model` | dbt `unique_id` (e.g. `model.my_project.my_model`) |
| `model_name` | Short model name |
| `materialized` | Materialization type: `table`, `view`, or `incremental` |
| `table_fqn` | Fully qualified table name (`catalog.schema.table`) |
| `database_name` | Catalog |
| `schema_name` | Schema |
| `table_name` | Table name |
| `run_count` | Number of successful runs in the lookback window |
| `avg_execution_time_s` | Average execution time per run in seconds |
| `max_execution_time_s` | Slowest single run in seconds |
| `min_execution_time_s` | Fastest single run in seconds |
| `total_execution_time_s` | Total compute time spent on this model in the lookback window |
| `avg_bytes_scanned_gb` | Average bytes scanned per run in GB |
| `max_bytes_scanned_gb` | Most bytes scanned in a single run |
| `total_bytes_scanned_gb` | Total bytes scanned across all runs in the lookback window |
| `estimated_monthly_runs` | Extrapolated monthly run count based on frequency in the lookback window |
| `estimated_monthly_bytes_scanned_gb` | Projected monthly bytes scanned if current patterns continue |
| `execution_time_trend_pct` | Percentage change in avg execution time from the first half to the second half of the lookback window. Positive = getting slower. Negative = getting faster. |
| `performance_trend` | `DEGRADING` (>20% slower), `IMPROVING` (>20% faster), `STABLE`, or `INSUFFICIENT_DATA` (model only ran in one half of the window) |
| `first_run` | Earliest run timestamp in the lookback window |
| `last_run` | Most recent run timestamp in the lookback window |

---

## Understanding `performance_trend`

The trend is calculated by splitting the lookback window in half and comparing average execution times:

```
trend_pct = (recent_avg_time - early_avg_time) / early_avg_time × 100
```

| Value | Meaning |
|-------|---------|
| `DEGRADING` | Execution time increased by more than 20% from the first half to the second half. Investigate source data growth, upstream model changes, or statistics staleness. |
| `IMPROVING` | Execution time decreased by more than 20%. This may reflect recent optimizations, reduced source volume, or caching effects. |
| `STABLE` | Execution time changed by less than 20% in either direction. |
| `INSUFFICIENT_DATA` | The model only ran during one half of the lookback window — not enough data to compute a trend. Widen `model_run_summary_lookback_days` or wait for more runs. |

**Note:** With a 7-day default lookback, each half is only ~3.5 days. Trend signals are more reliable with a 14- or 30-day window.

---

## Sample Queries

### Most expensive models by total compute this week

```sql
select
    model_name,
    materialized,
    run_count,
    avg_execution_time_s,
    total_execution_time_s,
    avg_bytes_scanned_gb,
    total_bytes_scanned_gb,
    performance_trend
from <your_catalog>.<your_schema>.fct_databricks__model_run_summary
where snapshot_date = current_date()
order by total_bytes_scanned_gb desc;
```

### Models with degrading performance

```sql
select
    model_name,
    table_fqn,
    avg_execution_time_s,
    execution_time_trend_pct,
    performance_trend,
    run_count,
    first_run,
    last_run
from <your_catalog>.<your_schema>.fct_databricks__model_run_summary
where snapshot_date = current_date()
    and performance_trend = 'DEGRADING'
order by execution_time_trend_pct desc;
```

### Projected monthly cost by model

```sql
select
    model_name,
    materialized,
    estimated_monthly_runs,
    estimated_monthly_bytes_scanned_gb,
    avg_execution_time_s
from <your_catalog>.<your_schema>.fct_databricks__model_run_summary
where snapshot_date = current_date()
order by estimated_monthly_bytes_scanned_gb desc;
```

### Performance trend over time for a specific model

```sql
select
    snapshot_date,
    avg_execution_time_s,
    avg_bytes_scanned_gb,
    run_count,
    execution_time_trend_pct,
    performance_trend
from <your_catalog>.<your_schema>.fct_databricks__model_run_summary
where model_name = 'my_model_name'
order by snapshot_date;
```

---

## Notes

- **Only current project models are captured.** Attribution uses the `node_id` from dbt's query comment, scoped to the current project. Ad-hoc SQL and other dbt projects are excluded.
- **`statement_text` must be populated.** If your workspace uses customer-managed keys, query text is redacted and this pipeline returns no rows.
- **View models show near-zero bytes scanned.** dbt creates views with a fast DDL statement. The bytes scanned shown for view models reflects the cost of the `CREATE OR REPLACE VIEW` operation, not the cost of querying the view.
- **Incremental snapshots.** One snapshot is produced per day. Running the model multiple times in the same day merges into the existing daily record.
