# Contributing to the dbt Cost Optimization Package

Thanks for your interest in improving this package! This guide explains how you can help today and what to expect when you do.

## How you can contribute right now

We're currently accepting **issues**: bug reports, feature requests, and questions. We aren't accepting outside pull requests yet. We plan to open the package to pull requests in the future, and this guide will be updated when we do.

### Report a bug

Open a new issue and choose the **Bug report** template. The most helpful bug reports include:

- The command you ran and the vars you set
- The full log output, including any error message
- Your data platform (and, for Snowflake, whether you're on Enterprise or Standard edition)
- The contents of your `packages.yml` and the output of `dbt --version`

Please remove anything sensitive (account identifiers, hostnames, table names) from logs before posting.

### Request a feature

Open a new issue and choose the **Feature request** template. Tell us about the cost or performance problem you want to detect, which data platforms it applies to, and who it would help. Concrete examples make it much easier for us to prioritize.

### Report a security vulnerability

**Please don't open a public issue for security vulnerabilities.** Instead, follow the instructions in this repository's security policy (the **Security** tab) to report it privately.

## What to expect

This package is maintained on a best-effort basis, without SLAs. We read every issue and will respond when we can, but we can't guarantee response times or that every request will be implemented.

## How the package is organized

Even before pull requests are open, it can help to know where things live when you're reporting an issue:

- **`models/<platform>/<layer>/`** holds each platform's models (for example `models/snowflake/marts/`). Models shared across platforms are in `models/shared/`.
- **`macros/optimizations/`** holds the commands you run with `dbt run-operation`, such as `find_table_clustering_candidates`.
- **`macros/platforms/<platform>/`** holds each platform's implementation of those macros. The package uses `adapter.dispatch`, so a command with the same name runs the right implementation for your data platform, or tells you it isn't available on your platform yet.
- **`macros/_macros.yml`** documents every macro's arguments and which platforms it's implemented for.
- **`dbt_project.yml`** lists every package var, grouped into shared and per-platform sections.
