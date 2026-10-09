---
name: run-mappings-locally
description: Run ingest view mappings against real ingest view results in staging to verify that all rows parse. Use when the user wants to test ingest mappings or an ingest view manifest against real raw data (e.g. "run the mappings for this view", "test this ingest view against staging", "does this mapping change parse all the real data?").
---

# Skill: Run Mappings Locally

## Overview

Run an ingest view's mappings (the manifest YAML) against the view's real
results in a GCP project to verify that every row parses. This catches
mapping errors that fixture-based tests miss, because it runs against the
*latest raw data* rather than hand-written fixtures. Use it when changing a
mapping YAML, changing an ingest view query, or debugging a parse failure in
the ingest pipeline.

## The command

```bash
python -m recidiviz.tools.ingest.development.run_mappings_against_ingest_view_results \
    --project-id recidiviz-staging \
    --state-code [STATE_CODE] \
    --ingest-view-name [INGEST_VIEW_NAME]
```

- Flags use **hyphens**, not underscores (`--project-id`, `--state-code`,
  `--ingest-view-name`).
- `--project-id` is required (`recidiviz-staging` or `recidiviz-123`; use
  staging unless the user explicitly asks for prod).
- `--state-code` is required, e.g. `US_ND`.
- `--ingest-view-name` is the **bare view name**: no `view_` prefix and no
  `.py` extension. For the view builder file
  `recidiviz/ingest/direct/regions/us_nd/ingest_views/view_elite_alias.py`,
  pass `--ingest-view-name elite_alias`. If the name is wrong, the script
  errors with a "Did you mean ...?" suggestion.

## Optional flags

- `--ingest-pipeline-type [ACTIVITY|IDENTITY]` — which pipeline's views and
  mappings to run. Defaults to `ACTIVITY` (views under the region's
  `ingest_views/` directory). Pass `IDENTITY` for views under the region's
  `identity_views/` directory.
- `--write-results` — also writes each parsed entity tree to a results file so
  you can inspect what the mapping produces. This slows the run down
  significantly; leave it off when you only need a pass/fail signal.

## What it does and where output goes

1. Builds the ingest view query and runs it against the project's latest
   PRIMARY raw data.
2. Validates that the query result schema matches the `input_columns` declared
   in the mapping YAML — a mismatch fails immediately with the missing /
   mismatched columns listed.
3. Parses every result row with the compiled manifest. Parse errors don't stop
   the run; each failing row and its traceback are appended to
   `mappings_errors.txt` in the system temp directory (the script logs the
   exact path at startup). With `--write-results`, parsed entity trees go to
   `mappings_results.txt` in the same directory.

A run that ends with "Parsed successfully." means every row parsed. Otherwise
read the errors file for the failing rows.

## Caveats

- The script only tests the view against the **latest** raw data. Bad raw data
  that a newer version of the row has since overwritten will not be caught.
- The query runs against real state data in BigQuery — results and error logs
  can contain PII. Never paste row contents into PRs, issues, or commit
  messages.

## Related Documentation

- `recidiviz/tools/ingest/development/run_mappings_against_ingest_view_results.py` —
  the script itself
- `recidiviz/ingest/direct/types/ingest_pipeline_type.py` — pipeline types and
  the per-region view/manifest subdirectories they map to
