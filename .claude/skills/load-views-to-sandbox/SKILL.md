---
name: load-views-to-sandbox
description: Load changed BigQuery views into a personal sandbox dataset and diff the results against the deployed views. Use when the user wants to test BQ view changes before merge (e.g. "load my views to a sandbox", "test this view change against real data", "compare my sandbox view output to what's deployed").
---

# Skill: Load Views to Sandbox

## Overview

Test changes to BigQuery views before merging by loading the changed views into
a set of temporary sandbox datasets, then (optionally) diffing the sandbox
results against the currently deployed views. This automates the procedure that
the "load_views_to_sandbox DA Tips" Notion pages document for humans — prefer
running the steps below over re-deriving them.

## Prerequisites

- **Rebase the branch on the latest deployed commit first.** The `auto` mode
  loads every view that differs from the last view deploy. If the branch is
  behind, views changed by *other people's* merged commits show up as "changed"
  and get loaded (and paid for) too. Rebase on the latest deployed version
  before loading so the sandbox contains only your changes.
- Google ADC credentials for the target project (default: `recidiviz-staging`).

## Step 1: Load changed views to the sandbox

```bash
python -m recidiviz.tools.load_views_to_sandbox \
    --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] \
    auto --load_changed_views_only
```

- `--sandbox_dataset_prefix` is required. Use something identifying, e.g. the
  user's first name plus a short change slug (`dana_pathways`).
- To only load rows for one state — much cheaper and faster — add
  `--state_code_filter [STATE_CODE]` (e.g. `US_XX`) *before* the `auto`
  keyword:

```bash
python -m recidiviz.tools.load_views_to_sandbox \
    --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] \
    --state_code_filter [STATE_CODE] \
    auto --load_changed_views_only
```

- All top-level flags (`--project_id`, `--state_code_filter`, `--prompt`,
  `--failure-mode`) must come **before** the `auto` / `manual` / `all`
  subcommand; subcommand-specific flags (like `--load_changed_views_only`)
  come after it.
- Without `--load_changed_views_only`, `auto` mode also loads views
  *downstream* of the changed views, which can be a very large graph. Start
  with `--load_changed_views_only` and expand only if needed (e.g. with
  `--load_up_to_addresses` / `--load_up_to_datasets`).
- For the full flag reference, run
  `python -m recidiviz.tools.load_views_to_sandbox --help` (and
  `... auto --help` / `... manual --help` for subcommand flags).

## Troubleshooting: 404 "Not found" for a parent view's materialized table

A sandbox load can fail with a BigQuery 404 like
`Not found: Table ..._materialized` when a loaded view queries the
materialized table of an ancestor view that was never materialized into the
sandbox. Fix it by targeting the **most downstream** view you care about in
`manual` mode and loading its ancestors:

```bash
python -m recidiviz.tools.load_views_to_sandbox \
    --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] \
    manual \
    --view_ids_to_load [DATASET_ID.VIEW_ID] \
    --update_ancestors True \
    --update_descendants False
```

This loads the target view plus every ancestor it depends on (so all
materialized dependencies exist in the sandbox), and skips the descendant
graph.

## Step 2: Diff sandbox results against the deployed view

Use `compare_views` to compare the sandbox copy of a view against the deployed
original:

```bash
python -m recidiviz.tools.calculator.compare_views \
    --dataset_original [DATASET_ID] \
    --dataset_new [SANDBOX_DATASET_PREFIX]_[DATASET_ID] \
    --view_id [VIEW_ID] \
    --output_dataset_prefix [OUTPUT_DATASET_PREFIX] \
    [--grouping_columns state_code]
```

- `--dataset_original`, `--dataset_new`, `--view_id`, and
  `--output_dataset_prefix` are required. The sandbox dataset name is the
  sandbox prefix + `_` + the original dataset id.
- Useful optional flags: `--view_id_new` (if the view was renamed),
  `--primary_keys` (comma-separated; otherwise the script guesses id-like
  columns), `--grouping_columns` (e.g. `state_code`) to group the diff stats,
  `--ignore_columns`, `--ignore_case`, and `--project_id`
  (default `recidiviz-staging`).
- Results land in
  `{project_id}.{output_dataset_prefix}_{dataset}_comparison_output.view_[full|differences]`
  and summary stats print to the console.

## Related Documentation

- `recidiviz/tools/load_views_to_sandbox.py` — full docstring with more
  invocation variants (`all` mode, `--changed_datasets_to_ignore`,
  source-table overrides, etc.)
- `recidiviz/tools/calculator/compare_views.py` — comparison output details
- Notion: "load_views_to_sandbox DA Tips" pages — the human-oriented
  walkthrough this skill automates
- [BigQuery Tooling](../../../recidiviz/big_query/CLAUDE.md)
