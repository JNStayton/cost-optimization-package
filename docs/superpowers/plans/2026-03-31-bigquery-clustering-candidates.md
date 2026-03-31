# BigQuery Clustering Candidates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a complete BigQuery clustering candidates pipeline plus a unified cross-platform fact model, without touching any existing Snowflake models.

**Architecture:** Six SQL files are created or updated: two existing BQ intermediate models are enriched to carry row counts, partition counts, and clustering key data; two new BQ intermediate models (`int_bigquery__table_inventory`, `int_bigquery__table_query_stats_daily`) complete the pipeline wired up in the router models; `fct_bigquery__table_clustering_candidates` is a BQ-native fact model (Option B) and `fct__table_clustering_candidates` is a unified cross-platform fact model (Option C) that branches on `target.type` for syntax and scoring differences.

**Tech Stack:** dbt (>=1.0.0), BigQuery SQL, Jinja2, `dbt.dateadd` / `dbt.type_float` cross-db macros.

**Spec:** `docs/superpowers/specs/2026-03-31-bigquery-clustering-candidates-design.md`

---

## File Map

| Action | File | Responsibility |
|---|---|---|
| Modify | `models/intermediate/bigquery/int_bigquery__tables.sql` | Add `row_count`, `clustering_key`, `is_deleted` via join to storage staging |
| Modify | `models/intermediate/bigquery/int_bigquery__table_storage.sql` | Pass through `total_rows`, `total_partitions` for inventory model |
| Create | `models/intermediate/bigquery/int_bigquery__table_inventory.sql` | Join tables + storage; produce `size_gb`, `is_already_clustered`, `approx_micropartitions` |
| Create | `models/intermediate/bigquery/int_bigquery__table_query_stats_daily.sql` | Daily query stats per table via query-text matching; Snowflake-compatible column names |
| Modify | `models/intermediate/_intermediate.yml` | Add column docs for the two new BQ intermediate models |
| Create | `models/marts/bigquery/fct_bigquery__table_clustering_candidates.sql` | Option B: BQ-native scoring on bytes billed + slot time |
| Create | `models/marts/fct__table_clustering_candidates.sql` | Option C: unified cross-platform model with Jinja scoring branch |

---

## Task 1: Enrich `int_bigquery__tables`

**Files:**
- Modify: `models/intermediate/bigquery/int_bigquery__tables.sql`

This model currently nulls out `row_count`, `clustering_key`, and `is_deleted`. We fix all three by joining `stg_bigquery__table_storage` (which has `total_rows`, `deleted`) and parsing `clustering_key` out of the DDL column.

- [ ] **Step 1: Replace the file contents**

```sql
{#--
  BigQuery table metadata. Joins stg_bigquery__table_storage to populate row_count,
  is_deleted, and clustering_key (parsed from DDL).

  Note: column names follow Snowflake convention; a future refactor could make them
  platform-neutral across all int_{platform}__tables models.
--#}
select
    t.table_catalog as database_name,
    t.table_schema as schema_name,
    t.table_name,
    cast(null as string) as table_id,
    t.table_type,
    -- total_rows from TABLE_STORAGE; INFORMATION_SCHEMA.TABLES does not expose row count
    s.total_rows as row_count,
    -- Extract clustering columns from DDL, e.g. "CLUSTER BY col1, col2\n"
    regexp_extract(t.ddl, r'(?i)CLUSTER BY (.+?)(?:\n|;|$)') as clustering_key,
    -- BigQuery has no transient table concept
    false as is_transient,
    -- deleted flag from TABLE_STORAGE (true while table is in time-travel window after deletion)
    coalesce(s.deleted, false) as is_deleted,
    'bigquery' as platform
from {{ ref('stg_bigquery__tables') }} as t
left join {{ ref('stg_bigquery__table_storage') }} as s
    on t.table_catalog = s.project_id
    and t.table_schema = s.table_schema
    and t.table_name = s.table_name
```

- [ ] **Step 2: Commit**

```bash
git add models/intermediate/bigquery/int_bigquery__tables.sql
git commit -m "feat(bigquery): enrich int_bigquery__tables with row_count, clustering_key, is_deleted"
```

---

## Task 2: Pass through `total_rows` and `total_partitions` in `int_bigquery__table_storage`

**Files:**
- Modify: `models/intermediate/bigquery/int_bigquery__table_storage.sql`

The inventory model needs `total_partitions` (used as the `approx_micropartitions` proxy for BigQuery) and `total_rows`. Both are already in the staging model — just not passed through.

- [ ] **Step 1: Replace the file contents**

```sql
{#--
  BigQuery table storage metrics. Passes through total_rows and total_partitions
  from stg_bigquery__table_storage for use by int_bigquery__table_inventory.

  Note: total_rows and total_partitions are BigQuery-specific extra columns beyond
  the standard int_table_storage schema. Column names follow Snowflake convention;
  a future refactor could make them platform-neutral.
--#}
select
    project_id as database_name,
    table_schema as schema_name,
    table_name,
    active_physical_bytes as active_bytes,
    time_travel_physical_bytes as time_travel_bytes,
    -- BigQuery has no failsafe storage concept (Snowflake-only)
    cast(null as int64) as failsafe_bytes,
    deleted as is_deleted,
    -- BigQuery-specific fields used by int_bigquery__table_inventory
    total_rows,
    total_partitions,
    'bigquery' as platform
from {{ ref('stg_bigquery__table_storage') }}
```

- [ ] **Step 2: Commit**

```bash
git add models/intermediate/bigquery/int_bigquery__table_storage.sql
git commit -m "feat(bigquery): pass through total_rows and total_partitions in int_bigquery__table_storage"
```

---

## Task 3: Create `int_bigquery__table_inventory`

**Files:**
- Create: `models/intermediate/bigquery/int_bigquery__table_inventory.sql`

This is the BQ analog of `int_snowflake__table_inventory`. It joins `int_bigquery__tables` + `int_bigquery__table_storage`, and produces the same output schema as the Snowflake version so the router model (`int_table_inventory`) and the unified fact model can use it interchangeably.

The key adaptation: `approx_micropartitions` = `total_partitions` (actual BQ partition count, not a bytes/16MB estimate).

- [ ] **Step 1: Create the file**

```sql
{{
  config(
    materialized='view',
    enabled=(target.type == 'bigquery')
  )
}}

{#--
  BigQuery table inventory: joins table metadata with storage metrics.
  Mirrors int_snowflake__table_inventory schema for cross-platform compatibility.

  Column name notes (Snowflake convention; could be made neutral in future):
    - approx_micropartitions: Snowflake = active_bytes / 16 MB (micropartition count approximation)
                              BigQuery  = total_partitions (actual date/range partition count)
--#}

select
    t.platform,
    t.database_name,
    t.schema_name,
    t.table_name,
    t.database_name || '.' || t.schema_name || '.' || t.table_name as table_fqn,
    t.table_type,
    case
        when t.table_type = 'MATERIALIZED VIEW' then 'Materialized View'
        else 'Permanent Table'
    end as normalized_table_type,
    coalesce(s.total_rows, 0) as row_count,
    t.clustering_key,
    t.clustering_key is not null as is_already_clustered,
    false as is_transient,
    s.active_bytes,
    s.active_bytes / pow(1024, 3) as size_gb,
    -- approx_micropartitions: BigQuery uses actual partition count as the data-density proxy.
    -- Snowflake equivalent is active_bytes / (16 * 1024 * 1024).
    coalesce(s.total_partitions, 0) as approx_micropartitions
from {{ ref('int_bigquery__tables') }} as t
inner join {{ ref('int_bigquery__table_storage') }} as s
    on t.database_name = s.database_name
    and t.schema_name = s.schema_name
    and t.table_name = s.table_name
where t.table_type in ('BASE TABLE', 'MATERIALIZED VIEW')
    and not coalesce(t.is_deleted, false)
    and not coalesce(s.is_deleted, false)
```

- [ ] **Step 2: Commit**

```bash
git add models/intermediate/bigquery/int_bigquery__table_inventory.sql
git commit -m "feat(bigquery): add int_bigquery__table_inventory"
```

---

## Task 4: Create `int_bigquery__table_query_stats_daily`

**Files:**
- Create: `models/intermediate/bigquery/int_bigquery__table_query_stats_daily.sql`

This is the BQ analog of `int_snowflake__table_query_stats_daily`. It produces one row per `(platform, stats_date, table_database, table_schema, table_name)` with daily aggregated query stats.

Key differences from Snowflake:
- Attribution via query-text `LIKE` matching (same fallback as Snowflake Standard edition; BQ has no `ACCESS_HISTORY` equivalent in the current staging model)
- `select_execution_time_ms_sum` = `total_slot_ms` (CPU-weighted slot time, not wall-clock)
- `select_partitions_scanned_sum` = `0` (not available at query level in BigQuery JOBS_BY_PROJECT)
- `select_partitions_total_sum` = `0` (score formula falls back to `approx_micropartitions` from inventory)
- `select_bytes_billed_sum` = `total_bytes_billed` (BQ-specific cost signal; 0 for Snowflake)

- [ ] **Step 1: Create the file**

```sql
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='table_query_stats_daily_key',
    on_schema_change='append_new_columns',
    enabled=(target.type == 'bigquery')
  )
}}

{#--
  Daily query statistics per BigQuery table.

  Attribution: query text LIKE '%table_name%' matching (same fallback as Snowflake Standard
  edition). BigQuery's referenced_tables field is not in stg_bigquery__jobs_by_project.

  Column name notes (Snowflake convention; could be made neutral in future):
    - select_execution_time_ms_sum: Snowflake = wall-clock execution_time_ms
                                    BigQuery  = total_slot_ms (parallel CPU-weighted time)
    - select_partitions_scanned_sum: Snowflake = partitions scanned per query
                                     BigQuery  = 0 (not available; table-level approx_micropartitions used in scoring)
    - select_partitions_total_sum:   Snowflake = total partitions available per query
                                     BigQuery  = 0 (not available; score formula falls back to approx_micropartitions)
    - select_bytes_billed_sum:       Snowflake = 0 (not applicable; Snowflake bills by compute)
                                     BigQuery  = total_bytes_billed (primary BQ cost signal)
--#}

{% set full_account = var('table_query_stats_full_account', false) %}
{% set initial_lookback_days = var('table_query_stats_initial_lookback_days', 7) %}

with candidate_tables as (
    {% if full_account %}
    select distinct
        platform,
        database_name as table_database,
        schema_name as table_schema,
        table_name
    from {{ ref('int_bigquery__table_inventory') }}
    {% else %}
    select distinct
        ti.platform,
        ti.database_name as table_database,
        ti.schema_name as table_schema,
        ti.table_name
    from {{ ref('int_bigquery__table_inventory') }} as ti
    inner join {{ ref('int_dbt__relations') }} as dm
        on ti.database_name = dm.database_name
        and ti.schema_name = dm.schema_name
        and ti.table_name = dm.table_name
    {% endif %}
),

query_history as (
    select
        query_id,
        cast(query_start_time as date) as stats_date,
        query_start_time,
        statement_type,
        -- execution_time_ms = total_slot_ms in int_bigquery__query_history
        execution_time_ms,
        -- bytes_scanned = total_bytes_billed in int_bigquery__query_history
        bytes_scanned,
        query_text
    from {{ ref('int_bigquery__query_history') }}
    where execution_status = 'SUCCESS'
    {% if is_incremental() %}
        and query_start_time >= timestamp_sub(
            (
                select coalesce(
                    max(cast(stats_date as timestamp)),
                    cast('1970-01-01' as timestamp)
                )
                from {{ this }}
            ),
            interval 1 day
        )
    {% else %}
        and query_start_time >= timestamp_sub(
            current_timestamp(),
            interval {{ initial_lookback_days }} day
        )
    {% endif %}
),

matched_queries as (
    select
        ct.platform,
        qh.stats_date,
        ct.table_database,
        ct.table_schema,
        ct.table_name,
        qh.statement_type,
        qh.execution_time_ms,
        qh.bytes_scanned
    from query_history as qh
    inner join candidate_tables as ct
        on qh.query_text like '%' || ct.table_name || '%'
)

select
    to_hex(md5(
        coalesce(platform, '') || '|' ||
        coalesce(cast(stats_date as string), '') || '|' ||
        coalesce(table_database, '') || '|' ||
        coalesce(table_schema, '') || '|' ||
        coalesce(table_name, '')
    )) as table_query_stats_daily_key,
    platform,
    stats_date,
    table_database,
    table_schema,
    table_name,
    count(*) as total_query_count,
    count(case when statement_type = 'SELECT' then 1 end) as select_count,
    count(
        case when statement_type in ('INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE_TABLE_AS_SELECT')
        then 1 end
    ) as dml_count,
    -- select_execution_time_ms_sum: BigQuery = total_slot_ms (parallel CPU time, not wall clock)
    sum(case when statement_type = 'SELECT' then coalesce(execution_time_ms, 0) else 0 end)
        as select_execution_time_ms_sum,
    -- select_partitions_scanned_sum: not available at query level in BigQuery JOBS_BY_PROJECT
    0 as select_partitions_scanned_sum,
    -- select_partitions_total_sum: not available; scoring falls back to approx_micropartitions
    0 as select_partitions_total_sum,
    -- select_bytes_billed_sum: BigQuery-specific primary cost signal (total_bytes_billed)
    -- In int_bigquery__query_history, bytes_scanned = total_bytes_billed
    sum(case when statement_type = 'SELECT' then coalesce(bytes_scanned, 0) else 0 end)
        as select_bytes_billed_sum,
    sum(coalesce(bytes_scanned, 0)) as bytes_scanned_sum,
    -- BigQuery has no local/remote spill concept
    0 as bytes_spilled_local_sum,
    0 as bytes_spilled_remote_sum
from matched_queries
group by 1, 2, 3, 4, 5, 6
```

- [ ] **Step 2: Commit**

```bash
git add models/intermediate/bigquery/int_bigquery__table_query_stats_daily.sql
git commit -m "feat(bigquery): add int_bigquery__table_query_stats_daily"
```

---

## Task 5: Document the two new BQ intermediate models in `_intermediate.yml`

**Files:**
- Modify: `models/intermediate/_intermediate.yml`

Append two model blocks after the existing `int_snowflake__table_query_stats_daily` block.

- [ ] **Step 1: Append the following YAML to the end of `_intermediate.yml`**

```yaml

  # ===========================================================================
  # int_bigquery__table_inventory
  # ===========================================================================
  - name: int_bigquery__table_inventory
    description: >
      BigQuery table inventory combining table metadata and storage metrics.
      Mirrors int_snowflake__table_inventory schema for cross-platform compatibility.
      Column names follow Snowflake convention; a future refactor could make them
      platform-neutral.
    columns:
      - name: platform
        description: Source data platform identifier (bigquery)
      - name: database_name
        description: BigQuery project ID
      - name: schema_name
        description: BigQuery dataset name
      - name: table_name
        description: BigQuery table name
      - name: table_fqn
        description: Fully-qualified table name (project.dataset.table)
      - name: table_type
        description: BigQuery native table type (BASE TABLE, MATERIALIZED VIEW)
      - name: normalized_table_type
        description: Human-readable table classification
      - name: row_count
        description: Total row count from TABLE_STORAGE
      - name: clustering_key
        description: Clustering columns parsed from DDL (e.g. "col1, col2"), null if unclustered
      - name: is_already_clustered
        description: Whether the table has a CLUSTER BY definition
      - name: is_transient
        description: Always false — BigQuery has no transient table concept
      - name: active_bytes
        description: Active physical storage in bytes (from TABLE_STORAGE)
      - name: size_gb
        description: Active storage converted to GB
      - name: approx_micropartitions
        description: >
          Data density proxy. BigQuery = total_partitions (actual date/range partition count
          from TABLE_STORAGE). Snowflake equivalent is active_bytes / 16 MB.

  # ===========================================================================
  # int_bigquery__table_query_stats_daily
  # ===========================================================================
  - name: int_bigquery__table_query_stats_daily
    description: >
      Incremental daily query statistics per BigQuery table. Attribution uses query-text
      LIKE matching (same fallback as Snowflake Standard edition; referenced_tables is not
      in stg_bigquery__jobs_by_project). Column names mirror int_snowflake__table_query_stats_daily
      for cross-platform compatibility. Column names follow Snowflake convention; a future
      refactor could make them platform-neutral.
    columns:
      - name: table_query_stats_daily_key
        description: Surrogate key for stats_date + table relation
      - name: platform
        description: Source data platform identifier (bigquery)
      - name: stats_date
        description: Date grain of aggregated query statistics
      - name: table_database
        description: BigQuery project ID
      - name: table_schema
        description: BigQuery dataset name
      - name: table_name
        description: BigQuery table name
      - name: total_query_count
        description: Total count of successful queries matched to the table
      - name: select_count
        description: Count of SELECT queries
      - name: dml_count
        description: Count of INSERT/UPDATE/DELETE/MERGE/CREATE_TABLE_AS_SELECT queries
      - name: select_execution_time_ms_sum
        description: >
          Sum of total_slot_ms for matched SELECT jobs. BigQuery equivalent of Snowflake
          execution_time_ms — represents parallel CPU-weighted time, not wall clock.
      - name: select_partitions_scanned_sum
        description: >
          Always 0 for BigQuery — partition scan counts are not available in JOBS_BY_PROJECT.
          Snowflake equivalent is actual partitions scanned. Scoring falls back to
          approx_micropartitions (total_partitions) from the inventory model.
      - name: select_partitions_total_sum
        description: >
          Always 0 for BigQuery — not available at query level. Scoring falls back to
          approx_micropartitions from the inventory model.
      - name: select_bytes_billed_sum
        description: >
          Sum of total_bytes_billed for matched SELECT jobs. BigQuery-specific cost signal.
          Set to 0 for Snowflake (Snowflake bills by compute time, not bytes).
          Used by fct_bigquery__table_clustering_candidates for BQ-native scoring.
      - name: bytes_scanned_sum
        description: Sum of total_bytes_billed across all matched queries (SELECT + DML)
      - name: bytes_spilled_local_sum
        description: Always 0 for BigQuery — no local spill concept
      - name: bytes_spilled_remote_sum
        description: Always 0 for BigQuery — no remote spill concept
```

- [ ] **Step 2: Commit**

```bash
git add models/intermediate/_intermediate.yml
git commit -m "docs: add _intermediate.yml docs for int_bigquery__table_inventory and int_bigquery__table_query_stats_daily"
```

---

## Task 6: Create `fct_bigquery__table_clustering_candidates` (Option B)

**Files:**
- Create: `models/marts/bigquery/fct_bigquery__table_clustering_candidates.sql`

BQ-native fact model. References the BQ-specific intermediate models directly (not through routers) so it has access to `select_bytes_billed_sum`. Score formula: `query_volume × avg_GB_billed_per_query × read_write_bonus × partition_density_multiplier`.

- [ ] **Step 1: Create the file**

```sql
{#--
  BigQuery clustering candidates (Option B: BigQuery-native).

  Scores tables using BigQuery's primary cost signals:
    - total_bytes_billed (how much each query costs)
    - total_slot_ms (compute intensity)
    - total_partitions (data density proxy for partition_density_multiplier)

  For a cross-platform unified version, see:
    models/marts/fct__table_clustering_candidates.sql
--#}
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='clustering_candidates_snapshot_key',
    enabled=(target.type == 'bigquery')
  )
}}

{% set lookback_days = var('clustering_candidates_lookback_days', 7) %}
{% set min_size_gb = var('clustering_candidates_min_size_gb', 1000) %}
{% set dbt_project_only = var('clustering_candidates_dbt_project_only', true) %}
{% set target_databases = var('clustering_candidates_target_databases', []) %}
{% set target_schemas = var('clustering_candidates_target_schemas', []) %}

with large_tables as (
    select
        ti.database_name,
        ti.schema_name,
        ti.table_name,
        ti.active_bytes as size_bytes,
        ti.size_gb,
        ti.row_count,
        ti.is_already_clustered,
        ti.approx_micropartitions,
        ti.normalized_table_type as table_type
    from {{ ref('int_bigquery__table_inventory') }} as ti
    where ti.size_gb >= {{ min_size_gb }}
        {% if target_databases and target_databases | length > 0 %}
            and ti.database_name in (
                {% for db in target_databases %}
                    '{{ db }}'{% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        {% endif %}
        {% if target_schemas and target_schemas | length > 0 %}
            and ti.schema_name in (
                {% for sc in target_schemas %}
                    '{{ sc }}'{% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        {% endif %}
    order by size_gb desc
    limit 100
),

table_query_stats as (
    select
        lt.database_name,
        lt.schema_name,
        lt.table_name,
        coalesce(sum(tqs.select_count), 0) as select_count,
        coalesce(sum(tqs.dml_count), 0) as dml_count,
        -- avg_slot_ms: average total_slot_ms per SELECT query (compute intensity signal)
        if(
            coalesce(sum(tqs.select_count), 0) > 0,
            coalesce(sum(tqs.select_execution_time_ms_sum), 0) / nullif(sum(tqs.select_count), 0),
            0
        ) as avg_slot_ms,
        -- avg_bytes_billed: average total_bytes_billed per SELECT query (primary cost signal)
        if(
            coalesce(sum(tqs.select_count), 0) > 0,
            coalesce(sum(tqs.select_bytes_billed_sum), 0) / nullif(sum(tqs.select_count), 0),
            0
        ) as avg_bytes_billed
    from large_tables as lt
    left join {{ ref('int_bigquery__table_query_stats_daily') }} as tqs
        on lt.database_name = tqs.table_database
        and lt.schema_name = tqs.table_schema
        and lt.table_name = tqs.table_name
        and tqs.stats_date >= date_sub(current_date(), interval {{ lookback_days }} day)
    group by 1, 2, 3
),

scored as (
    select
        current_timestamp() as analyzed_at,
        lt.database_name,
        lt.schema_name,
        lt.table_name,
        lt.database_name || '.' || lt.schema_name || '.' || lt.table_name as table_fqn,
        dm.dbt_model,
        lt.table_type,
        coalesce(tqs.select_count, 0) as select_count,
        coalesce(tqs.dml_count, 0) as dml_count,
        coalesce(tqs.avg_slot_ms, 0) as avg_slot_ms,
        coalesce(tqs.avg_bytes_billed, 0) as avg_bytes_billed,
        lt.size_gb,
        coalesce(lt.row_count, 0) as row_count,
        -- approx_micropartitions = total_partitions for BigQuery
        lt.approx_micropartitions as total_partitions
    from large_tables as lt
    left join table_query_stats as tqs
        on lt.database_name = tqs.database_name
        and lt.schema_name = tqs.schema_name
        and lt.table_name = tqs.table_name
    left join {{ ref('int_dbt__relations') }} as dm
        on lt.database_name = dm.database_name
        and lt.schema_name = dm.schema_name
        and lt.table_name = dm.table_name
),

final as (
    select
        current_timestamp() as analyzed_at,
        current_date() as snapshot_date,
        to_hex(md5(
            cast(current_date() as string) || '|' || coalesce(table_fqn, '')
        )) as clustering_candidates_snapshot_key,
        database_name,
        schema_name,
        table_name,
        table_fqn,
        dbt_model,
        table_type,
        -- Score: query volume × avg GB billed per query × read-heavy bonus × partition density
        -- Higher = more bytes billed per query, more frequent reads, more partitions per row
        (
            case
                when select_count > 0 then
                    -- avg GB billed per query (primary BigQuery cost signal)
                    (select_count * (avg_bytes_billed / pow(1024, 3)))
                    -- read-heavy bonus: tables queried far more than written benefit most
                    + ((select_count / if(dml_count = 0, 1, dml_count)) * 10)
                else 0
            end
        )
        * (
            -- partition density multiplier: many partitions relative to row count
            -- suggests fragmented data that clustering can consolidate
            case
                when row_count > 0 and total_partitions > 0
                    then greatest(cast(total_partitions as float64) / row_count * 1000, 1)
                else 1
            end
        ) as score,
        case
            when
                select_count > 0
                and (select_count / if(dml_count = 0, 1, dml_count)) > 1
                and size_gb >= {{ min_size_gb }}
            then true
            else false
        end as is_candidate,
        size_gb as table_size_gb,
        row_count as total_rows,
        total_partitions as current_partitions,
        round(avg_bytes_billed / pow(1024, 3), 4) as avg_gb_billed_per_query,
        select_count,
        dml_count,
        round(cast(select_count as float64) / (dml_count + 1), 1) as query_to_dml_ratio,
        round(avg_slot_ms / 1000, 2) as avg_slot_seconds
    from scored
    where
        {% if dbt_project_only %}
            dbt_model is not null
        {% else %}
            1 = 1
        {% endif %}
)

select
    analyzed_at,
    snapshot_date,
    clustering_candidates_snapshot_key,
    database_name,
    schema_name,
    table_name,
    table_fqn,
    dbt_model,
    table_type,
    score,
    is_candidate,
    table_size_gb,
    total_rows,
    current_partitions,
    avg_gb_billed_per_query,
    select_count,
    dml_count,
    query_to_dml_ratio,
    avg_slot_seconds
from final
{% if is_incremental() %}
where snapshot_date >= (
    select coalesce(max(snapshot_date), date('1970-01-01'))
    from {{ this }}
)
{% endif %}
```

- [ ] **Step 2: Commit**

```bash
git add models/marts/bigquery/fct_bigquery__table_clustering_candidates.sql
git commit -m "feat(bigquery): add fct_bigquery__table_clustering_candidates (Option B)"
```

---

## Task 7: Create `fct__table_clustering_candidates` (Option C — unified)

**Files:**
- Create: `models/marts/fct__table_clustering_candidates.sql`

Cross-platform fact model. Uses `int_table_inventory` and `int_table_query_stats_daily` (the platform-agnostic router models). Uses `{{ dbt.dateadd() }}` for date arithmetic. Uses `CASE WHEN` instead of `iff()`. Uses Jinja for `md5` encoding (platform-specific) and score formula (BQ branch vs Snowflake/others branch).

`select_bytes_billed_sum` flows through the router's `select *` for BigQuery, so the BQ score branch can reference it. For Snowflake and others it will not exist in the output — the Jinja conditional ensures it's never referenced on those platforms.

- [ ] **Step 1: Create the file**

```sql
{#--
  Unified cross-platform clustering candidates (Option C).

  Works on Snowflake, BigQuery, Redshift, and Databricks by routing through
  platform-agnostic intermediate models. Jinja handles:
    1. SQL syntax differences (md5, date functions)
    2. Platform-specific scoring (BQ uses bytes billed; others use execution time + partitions)

  For a BigQuery-native version with more granular BQ metrics, see:
    models/marts/bigquery/fct_bigquery__table_clustering_candidates.sql
--#}
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='clustering_candidates_snapshot_key'
  )
}}

{% set lookback_days = var('clustering_candidates_lookback_days', 7) %}
{% set min_size_gb = var('clustering_candidates_min_size_gb', 1000) %}
{% set dbt_project_only = var('clustering_candidates_dbt_project_only', true) %}
{% set target_databases = var('clustering_candidates_target_databases', []) %}
{% set target_schemas = var('clustering_candidates_target_schemas', []) %}

with large_tables as (
    select
        ti.database_name,
        ti.schema_name,
        ti.table_name,
        ti.active_bytes as size_bytes,
        ti.size_gb,
        ti.row_count,
        ti.is_already_clustered,
        ti.approx_micropartitions,
        ti.normalized_table_type as table_type
    from {{ ref('int_table_inventory') }} as ti
    where ti.size_gb >= {{ min_size_gb }}
        {% if target_databases and target_databases | length > 0 %}
            and upper(ti.database_name) in (
                {% for db in target_databases %}
                    '{{ db | upper }}'{% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        {% endif %}
        {% if target_schemas and target_schemas | length > 0 %}
            and upper(ti.schema_name) in (
                {% for sc in target_schemas %}
                    '{{ sc | upper }}'{% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        {% endif %}
    order by size_gb desc
    limit 100
),

table_query_stats as (
    select
        lt.database_name,
        lt.schema_name,
        lt.table_name,
        coalesce(sum(tqs.select_count), 0) as select_count,
        coalesce(sum(tqs.dml_count), 0) as dml_count,
        case
            when coalesce(sum(tqs.select_count), 0) > 0
            then coalesce(sum(tqs.select_execution_time_ms_sum), 0)
                 / nullif(sum(tqs.select_count), 0)
            else 0
        end as avg_execution_time_ms,
        case
            when coalesce(sum(tqs.select_count), 0) > 0
            then coalesce(sum(tqs.select_partitions_scanned_sum), 0)
                 / nullif(sum(tqs.select_count), 0)
            else 0
        end as avg_partitions_scanned,
        case
            when coalesce(sum(tqs.select_count), 0) > 0
            then coalesce(sum(tqs.select_partitions_total_sum), 0)
                 / nullif(sum(tqs.select_count), 0)
            else 0
        end as avg_partitions_total{% if target.type == 'bigquery' %},
        -- avg_bytes_billed: BigQuery-only cost signal flowing through the router's select *
        case
            when coalesce(sum(tqs.select_count), 0) > 0
            then coalesce(sum(tqs.select_bytes_billed_sum), 0)
                 / nullif(sum(tqs.select_count), 0)
            else 0
        end as avg_bytes_billed
        {% endif %}
    from large_tables as lt
    left join {{ ref('int_table_query_stats_daily') }} as tqs
        on upper(lt.database_name) = upper(tqs.table_database)
        and upper(lt.schema_name) = upper(tqs.table_schema)
        and upper(lt.table_name) = upper(tqs.table_name)
        and tqs.stats_date >= {{ dbt.dateadd('day', -lookback_days, 'current_date()') }}
    group by 1, 2, 3
),

scored as (
    select
        current_timestamp() as analyzed_at,
        lt.database_name,
        lt.schema_name,
        lt.table_name,
        upper(lt.database_name) || '.' || upper(lt.schema_name) || '.' || upper(lt.table_name) as table_fqn,
        dm.dbt_model,
        lt.table_type,
        coalesce(tqs.select_count, 0) as select_count,
        coalesce(tqs.dml_count, 0) as dml_count,
        coalesce(tqs.avg_execution_time_ms, 0) as avg_execution_time_ms,
        coalesce(tqs.avg_partitions_scanned, 0) as avg_partitions_scanned,
        coalesce(tqs.avg_partitions_total, 0) as avg_partitions_total,
        {% if target.type == 'bigquery' %}
        coalesce(tqs.avg_bytes_billed, 0) as avg_bytes_billed,
        {% endif %}
        lt.size_gb,
        coalesce(lt.row_count, 0) as row_count,
        -- micropartitions: falls back to approx_micropartitions when avg_partitions_total = 0
        -- For BigQuery: avg_partitions_total = 0, so always uses approx_micropartitions = total_partitions
        coalesce(
            nullif(coalesce(tqs.avg_partitions_total, 0), 0),
            lt.approx_micropartitions
        ) as micropartitions
    from large_tables as lt
    left join table_query_stats as tqs
        on upper(lt.database_name) = upper(tqs.database_name)
        and upper(lt.schema_name) = upper(tqs.schema_name)
        and upper(lt.table_name) = upper(tqs.table_name)
    left join {{ ref('int_dbt__relations') }} as dm
        on upper(lt.database_name) = upper(dm.database_name)
        and upper(lt.schema_name) = upper(dm.schema_name)
        and upper(lt.table_name) = upper(dm.table_name)
),

final as (
    select
        current_timestamp() as analyzed_at,
        current_date() as snapshot_date,
        -- Surrogate key: platform-specific md5 encoding
        {% if target.type == 'bigquery' %}
        to_hex(md5(cast(current_date() as string) || '|' || coalesce(table_fqn, '')))
        {% elif target.type == 'snowflake' %}
        md5(to_varchar(current_date()) || '|' || coalesce(table_fqn, ''))
        {% else %}
        md5(cast(current_date() as varchar) || '|' || coalesce(table_fqn, ''))
        {% endif %}
            as clustering_candidates_snapshot_key,
        database_name,
        schema_name,
        table_name,
        table_fqn,
        dbt_model,
        table_type,
        (
            case
                when select_count > 0 then
                    {% if target.type == 'bigquery' %}
                    -- BigQuery: score on avg GB billed per query (primary cost signal)
                    -- Higher bytes billed = more data scanned = more benefit from clustering
                    (select_count * (avg_bytes_billed / pow(1024, 3)))
                    {% else %}
                    -- Snowflake/others: score on execution time (avg seconds per query × volume)
                    (select_count * (avg_execution_time_ms / 1000))
                    {% endif %}
                    -- read-heavy bonus: shared across all platforms
                    + ((select_count / case when dml_count = 0 then 1 else dml_count end) * 10)
                else 0
            end
        )
        * (
            -- partition density multiplier: shared across all platforms
            -- micropartitions = total_partitions for BigQuery (via approx_micropartitions fallback)
            case
                when row_count > 0 and (micropartitions / row_count) * 100 > 0.0001
                    then (micropartitions / row_count) * 100
                else 1
            end
        ) as score,
        case
            when
                select_count > 0
                and (select_count / case when dml_count = 0 then 1 else dml_count end) > 1
                and size_gb >= {{ min_size_gb }}
            then true
            else false
        end as is_candidate,
        size_gb as table_size_gb,
        row_count as total_rows,
        micropartitions as current_micropartitions,
        case
            when micropartitions > 0 then round(row_count / micropartitions, 2)
            else 0
        end as avg_rows_per_micropartition,
        avg_partitions_scanned,
        select_count,
        dml_count,
        round(select_count / (dml_count + 1), 1) as query_to_dml_ratio,
        round(avg_execution_time_ms / 1000, 2) as avg_query_duration_s
    from scored
    where
        {% if dbt_project_only %}
            dbt_model is not null
        {% else %}
            1 = 1
        {% endif %}
)

select
    analyzed_at,
    snapshot_date,
    clustering_candidates_snapshot_key,
    database_name,
    schema_name,
    table_name,
    table_fqn,
    dbt_model,
    table_type,
    score,
    is_candidate,
    table_size_gb,
    total_rows,
    current_micropartitions,
    avg_rows_per_micropartition,
    avg_partitions_scanned,
    select_count,
    dml_count,
    query_to_dml_ratio,
    avg_query_duration_s
from final
{% if is_incremental() %}
where snapshot_date >= (
    select coalesce(
        max(snapshot_date),
        cast('1970-01-01' as date)
    )
    from {{ this }}
)
{% endif %}
```

- [ ] **Step 2: Commit**

```bash
git add models/marts/fct__table_clustering_candidates.sql
git commit -m "feat: add fct__table_clustering_candidates unified cross-platform model (Option C)"
```

---

## Validation

These models require a live BigQuery connection to fully validate. The recommended approach is to install the package in a real dbt project pointed at a BigQuery warehouse:

```yaml
# packages.yml in your test project
packages:
  - local: /path/to/dbt-cost-optimization-package
```

Then run:

```bash
# Verify all BQ models compile (no DB connection needed for parse)
dbt parse --target bigquery

# Compile to inspect generated SQL before running
dbt compile --select int_bigquery__table_inventory --target bigquery
dbt compile --select int_bigquery__table_query_stats_daily --target bigquery
dbt compile --select fct_bigquery__table_clustering_candidates --target bigquery
dbt compile --select fct__table_clustering_candidates --target bigquery

# Full run against live BQ (requires credentials)
dbt run --select int_bigquery__table_inventory --target bigquery
dbt run --select int_bigquery__table_query_stats_daily --target bigquery
dbt run --select fct_bigquery__table_clustering_candidates --target bigquery
dbt run --select fct__table_clustering_candidates --target bigquery

# Verify unified model also compiles for Snowflake (no changes to SF models)
dbt parse --target snowflake
dbt compile --select fct__table_clustering_candidates --target snowflake
```
