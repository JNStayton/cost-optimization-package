# Dashboard (dbt-charts, Snowflake only)

A [dbt-charts](https://github.com/dbt-labs/dbt-charts) board visualizing
this package's Snowflake gold-layer output: savings by domain and effort
category, the ranked optimization backlog, per-model recommendations,
warehouse optimizations, and cross-domain insights.

Snowflake only for now. Other platforms have differently-shaped gold
layers, so this board does not generalize to them as-is.

## Setup

1. Install dbt-charts (requires Python <3.14; `dbt-charts>=0.8.0`, since
   `ref()` resolution inside board queries is broken before that version):

   ```bash
   pip install "dbt-charts[snowflake]"
   ```

2. Build this package's models, from your project root. Select the whole
   package (`package:dbt_cost_optimization`), not just `tag:gold` -
   the gold views are tagged `gold`, but the fact tables they read from
   (clustering, materialization, warehouse, AI spend) each carry their own
   domain tag instead. Selecting the whole package builds everything this
   package can produce in one pass, rather than relying on `+tag:gold`'s
   ancestor selection to pull in exactly the right set:

   ```bash
   dbt build --vars '{dbt_cost_optimization_enabled: true}' --select package:dbt_cost_optimization
   ```

3. Set your dbt profile name as an environment variable, once, in your
   shell profile or CI env (the same profile you already use for `dbt build`):

   ```bash
   export DBT_COST_OPT_PROFILE=<your dbt profile name>
   ```

   This does not go in a file: `dbt_packages/` is fully wiped and
   re-cloned by every `dbt deps`, so anything written into a file there is
   silently lost on the next install. The env var lives outside
   `dbt_packages/` and survives every future `dbt deps`.

4. Launch:

   ```bash
   cd dbt_packages/dbt_cost_optimization/integrations/dbt_charts
   dct validate
   dct serve
   ```

   `dct serve` prints the URL it's bound to (defaults to
   `http://localhost:8501/snowflake_cost_overview/`). To render a static
   snapshot instead of serving live:

   ```bash
   dct render charts/snowflake_cost_overview.yml --format html --output /path/to/output.html
   ```

## If your project uses a custom package install path

If `packages-install-path` in your `dbt_project.yml` is not the default
`dbt_packages`, the relative path this board uses to find your project's
`dbt_project.yml` and manifest will not resolve. Point dct at your project
root explicitly instead:

```bash
dct render charts/snowflake_cost_overview.yml --dbt-project-dir /path/to/your/project
```

(or set the `DBT_PROJECT_DIR` environment variable to the same path).
