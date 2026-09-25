---
name: Bug report
about: Report a bug or an issue you've found with this package
title: ''
labels: bug, triage
assignees: ''

---

### Describe the bug
<!---
A clear and concise description of what the bug is. You can also use the issue title to do this.
--->

### Steps to reproduce
<!---
In as much detail as possible, please provide steps to reproduce the issue: the command you ran
(e.g. `dbt build --select package:dbt_cost_optimization_package`, or
`dbt run-operation find_spillage_candidates --args '{...}'`), the vars you set, and any
other configuration that matters.
--->

### Expected results
<!---
A clear and concise description of what you expected to happen.
--->

### Actual results
<!---
A clear and concise description of what actually happened.
--->

### Log output
<!---
Please paste the full log output for the failing command, including any error message.
Remove anything sensitive (account identifiers, hostnames, table names) before posting.
--->
```
<log output goes here>
```

### System information
**Which data platform are you using?**
- [ ] Snowflake — Enterprise edition
- [ ] Snowflake — Standard edition
- [ ] Databricks
- [ ] BigQuery
- [ ] Redshift

**Which model, macro, or command is affected?**
<!--- e.g. fct_snowflake__table_clustering_candidates, suggest_clustering_keys --->

**The contents of your `packages.yml` file** (including the package version or revision):
```yaml
<packages.yml goes here>
```

**Any package vars you've overridden** (in your `vars.yml` or with `--vars`):
```yaml
<vars go here>
```

**The output of `dbt --version`** (dbt Core or Fusion):
```
<output goes here>
```

### Additional context
<!---
Add any other context about the problem here. For example, if you think you know which line of code is causing the issue.
--->

### Are you interested in contributing the fix?
<!---
Let us know if you'd like to contribute the fix once we're accepting pull requests, and whether you'd need a hand getting started.
--->
