# dbt-cost-optimization-package

A dbt package built by dbt Labs that helps data teams identify cost optimization opportunities across multiple data platforms (Snowflake, Databricks, Redshift, BigQuery).

---

## Tools (preferred order)

### dbt MCP — use first for anything dbt-related
Use for: model lineage, job/run status, compiling SQL, querying dbt Cloud metadata.

### File tools — for editing models, YAMLs, and configs
Read and edit `.sql` and `.yml` files directly when making changes to the project.

---

## Project structure

```
models/
  staging/
    snowflake/     # stg_snowflake__* — raw Snowflake system table access
    databricks/    # stg_databricks__* — raw Databricks system table access
    redshift/      # stg_redshift__* — raw Redshift system table access
    bigquery/      # stg_bigquery__* — raw BigQuery system table access
  intermediate/
    snowflake/     # int_snowflake__* — Snowflake-specific transformations
    databricks/    # int_databricks__* — Databricks-specific transformations
    redshift/      # int_redshift__* — Redshift-specific transformations
    bigquery/      # int_bigquery__* — BigQuery-specific transformations
    (root)         # int_* — cross-platform normalized models
  marts/
    snowflake/     # fct_snowflake__* — Snowflake cost optimization outputs
    databricks/    # fct_databricks__* — Databricks cost optimization outputs
```

## dbt Cloud connection

- **Host:** https://tk626.us1.dbt.com
- **Account ID:** 51798

---

## Conventions

- Models are organized by platform within each layer
- Staging models clean and rename raw system table columns only
- Intermediate models apply business logic per platform
- Mart models (`fct_`) are the final outputs — one fact per optimization recommendation type
- Cross-platform intermediate models (no subfolder) normalize data across platforms

## Notes for Claude

- This is a dbt Labs internal package, not a client project
- Changes here affect all platforms — check impact across Snowflake, Databricks, Redshift, and BigQuery before modifying shared intermediate models
- Never include credentials or tokens in any file other than `.env`
- This is a read-only exploration tool — do not create, edit, or delete files unless explicitly asked
