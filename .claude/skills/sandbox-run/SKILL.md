---
name: sandbox-run
description: Run a BigQuery sandbox to validate pipeline or view changes before merge. Picks between load_end_to_end_data_sandbox (ingest, normalization or pipeline changes) and load_views_to_sandbox (view-only changes), builds the command with the right flag order, runs an end-to-end sandbox unattended with --unattended, chains view-only loads off an end-to-end sandbox's outputs, and diffs results with compare_views. Use when the user mentions a sandbox, says "load views to sandbox", "load my views to a sandbox", "end-to-end sandbox", "load_views_to_sandbox", "load_end_to_end_data_sandbox", "test this view change against real data", "validate my ingest change", "which sandbox tool should I use", "did every view get loaded", "compare sandbox output", or "compare my sandbox view output to what's deployed".
---

# Skill: Run Sandbox

## Overview

Two tools build BigQuery sandboxes from a branch's code:

- `load_views_to_sandbox` rebuilds BigQuery views only. Minutes to an hour.
- `load_end_to_end_data_sandbox` rebuilds the Dataflow pipelines (ingest, metrics,
  supplemental) for one state, then the views on top of them. 1-3 hours.

It covers four jobs: choosing between the tools, a views-only load, an
unattended end-to-end run, and loading views on top of an end-to-end run's
outputs. Comparing the results with `compare_views` is an optional last step.

By default, hand the user the command to run themselves; the tools consume
shared compute. When the user asks Claude to run an end-to-end sandbox, use
`--unattended` (below). Never pipe `yes` into either tool: after launching
each Dataflow pipeline the end-to-end tool asks "Has it completed?", and
answering before the job finishes starts the next stage on unfinished data.

## Which mode

Neither tool decides this. Classify the branch's diff:

```bash
git diff --name-only $(git merge-base HEAD origin/main)
```

| Any changed file under | Mode | Why |
|---|---|---|
| `recidiviz/ingest/direct/regions/*/ingest_views/`, `.../ingest_mappings/`, `.../raw_data/`, `recidiviz/pipelines/`, `recidiviz/persistence/entity/`, or a normalization delegate under `recidiviz/pipelines/utils/state_utils/` | End-to-end | These change `normalized_state` or the Dataflow outputs; a views-only load would read the stale deployed versions. Ingest views read raw tables through the `raw_data/` configs' primary keys, columns and ordering, so a raw config edit counts unless it only changes descriptions |
| Anything else (view SQL, view builders, Python constants that feed SQL) | Views-only `auto` | The pipelines are unaffected |

Three situations change that answer:

| Situation | Mode |
|---|---|
| No uncommitted code to measure; comparing deployed datasets | No sandbox. Query staging or prod. Staging deploys ahead of prod, so say which project a number came from. |
| The pipeline change is already in the last staging deploy | Views-only against staging; its `normalized_state` is already correct. The rebase pre-flight below shows the deployed version. |
| Iterating on views while an end-to-end run's tables last (24 hours from when each was created, so count from its ingest stage) | Views-only reading that run's outputs. See "Views off an end-to-end sandbox". |

Present this as a recommendation: say which rule fired and which files
triggered it, and let the user pick the other tool if they prefer, for example
views-only for speed, knowing the views read the deployed pipeline outputs.

## Pre-flight (both tools)

1. Credentials: `gcloud auth login --update-adc`. Both tools exit with that
   message when ADC has expired. If the views to load read Google Sheets
   (TN's `validation_views.us_tn_loopback_*` and
   `analyst_data.workflows_live_completion_event_types_by_state` do), use
   `gcloud auth login --enable-gdrive-access --update-adc` instead, or those
   views fail with a Drive permission error.
2. Dataflow access, end-to-end only. Seeing Dataflow jobs needs the
   `dataflow.jobs.list` and `dataflow.jobs.get` permissions on the project;
   launching them does not, so a run without them launches jobs nobody can
   then follow. This read-only command fails with `Permission
   'dataflow.jobs.list' denied` without them:
   ```bash
   gcloud dataflow jobs list --project=recidiviz-staging --limit=1
   ```
   If it fails, ask the Data Platform team for access. `--unattended` runs the
   same check before it starts and stops without it.
3. End-to-end only: `git config user.name` is set. It names the Dataflow image,
   `dataflow-dev/{name lowercased, no spaces}/build:latest`, one per person.
4. Rebase. `auto` mode and the end-to-end tool exit 1, with no prompt, unless
   the branch contains the commit of the last successful staging view update:
   ```bash
   uv run python -m recidiviz.tools.deploy.print_latest_view_update_version --project_id recidiviz-staging
   ```
   Staging deploys during the day, so a branch cut in the morning can fail
   this after an afternoon deploy. Rebasing onto main's tip fixes it, but
   everything merged since the deploy then counts as changed and loads too;
   rebasing onto the deployed commit itself keeps the sandbox to your changes.
   This command only reads. The tools' own rebase check also runs `git fetch
   --all --tags --prune --prune-tags --force`, which deletes local tags that
   are not on the remote. Worktrees share one repository's refs, so two runs
   fetching at once can fail one of them with `cannot lock ref`; see "Launch
   order" below.
5. Prefix: `{username}{MMDDYY}{xx}`, where `xx` is a short tag of your
   choosing, conventionally the state's two letters plus a letter per run when
   there is more than one (`agaidus100526nd`, `agaidus100726oza`). A `us_xx`
   token in the prefix (`agaidus_us_nd`) is different: it must match the state
   filter. The end-to-end tool only checks that at its last stage (stage 7,
   loading views), after the pipelines; `--unattended` also checks it before
   starting.
6. The worktree's checked-out code is what runs, so launch from the
   worktree that has the change, after `uv sync --all-extras` there. Do not
   switch branches during the image build.
7. Sandbox tables expire 24 hours after creation. Plan any views-off-e2e load
   and any `compare_views` within that day.

## Views-only: `load_views_to_sandbox`

Top-level flags go before the `auto` / `manual` / `all` subcommand; the
subcommand's own flags go after it. Top-level: `--project_id`,
`--sandbox_dataset_prefix` (required), `--state_code_filter`, `--prompt`,
`--failure-mode`, `--input_source_table_dataset_overrides_json`,
`--allow_slow_views`, `--rematerialize_changed_views_only`.

Auto, only the changed views. Start here and widen only if needed:

```bash
caffeinate -i uv run python -m recidiviz.tools.load_views_to_sandbox \
    --sandbox_dataset_prefix {prefix} --state_code_filter US_XX --prompt \
    auto --load_changed_views_only
```

Auto, the changed views and everything between them and a product surface:

```bash
caffeinate -i uv run python -m recidiviz.tools.load_views_to_sandbox \
    --sandbox_dataset_prefix {prefix} --state_code_filter US_XX --prompt \
    auto --load_up_to_datasets workflows_views,task_eligibility_spans_us_xx
```

Manual, a named view plus its ancestors:

```bash
caffeinate -i uv run python -m recidiviz.tools.load_views_to_sandbox \
    --sandbox_dataset_prefix {prefix} --state_code_filter US_XX --prompt \
    manual --view_ids_to_load {dataset}.{view} --update_ancestors True --update_descendants False
```

What auto loads: the changed views, the end views, and every view on a path
between them. The end views are the changed views themselves under
`--load_changed_views_only`; otherwise they are the `--load_up_to_addresses`
views plus every view in each `--load_up_to_datasets` dataset, including views
the change does not touch. One of the three is required, and
`--load_changed_views_only` cannot be combined with `--load_up_to_*`.

Auto is the default: it does the change detection. Manual does none, skips the
rebase check, and the tool warns there are "relatively few known use cases"
for it. Manual is for two jobs:

- Baselines. Auto on the branch's merge base finds nothing changed. Run auto
  with `--prompt` on the branch, answer `n` to capture the printed list, then
  replay that list with `manual --view_ids_to_load {list}
  --update_descendants False` on both the merge base and the branch, so the
  two sides load the identical set. The printed list already runs from the
  changed views to the end views; manual's default `--update_descendants True`
  would add every descendant of every listed view, with no cutoff at the end
  views. `--update_ancestors` defaults to False, which keeps both sides on the
  same deployed parents.
- Targeted reloads: one downstream view with its ancestors (the 404 fix
  below), or an unchanged view whose inputs moved.

## How to know every relevant view loads

Auto compares each view's locally rendered SQL, schema, clustering and
partitioning against the signatures BigQuery recorded at the last successful
view deploy (`view_update_metadata.per_view_update_stats`). It does not diff
git. So:

- A Python constant that changes a view's rendered SQL is detected.
- A change only to a description is not.
- Anything merged to main after the last deploy shows up as changed. Drop that
  noise with `--changed_datasets_to_ignore`, rebase onto the deployed commit
  (pre-flight 4), or wait for the next deploy.
- The warning that no view update has succeeded in over 24 hours means the
  baseline is stale; expect extra changes.

The tool prints a summary table (IGNORED / ADDED / UPDATED / DOWNSTREAM, plus
overridden source tables) and the view list either way. `--prompt` is what
makes the views tool stop at "Continue with loading N views?" so you can read
the list before it spends the hour; the end-to-end tool always stops there.
Check three things: every view you edited is under ADDED or UPDATED; the
product views you care about appear; N is plausible for one state. An
end-to-end run for an ingest-only change edits no view: the change shows up as
the overridden source tables, and every view that reads them is DOWNSTREAM, so
check the second and third items. ADDED or UPDATED views you never touched are
merges since the last deploy (see above). To drop them, answer `n` and relaunch
with `--changed_datasets_to_ignore` or `--changed_addresses_to_ignore`, and
pass the same flags to the baseline run so both sides load the same views.
Other questions fire with or without `--prompt`, and there is no `--yes` flag: the
`all` and `manual` warnings, the 24-hour warning, "Continue anyway?" when
`--changed_addresses_to_include` names unchanged views, and the expensive
union-all warning when no state filter is set.

`Not found: Table ..._materialized` during a load means a loaded view reads an
ancestor that was never materialized into the sandbox. Load the most
downstream view you care about in manual mode with `--update_ancestors True
--update_descendants False`.

## End-to-end: `load_end_to_end_data_sandbox`

```bash
caffeinate -i uv run python -m recidiviz.tools.load_end_to_end_data_sandbox \
    --state_code US_XX --sandbox_prefix {prefix} \
    --load_up_to_datasets sentence_sessions,workflows_views
```

| Argument | Note |
|---|---|
| `--state_code` | Required. The views tool calls this `--state_code_filter`. |
| `--sandbox_prefix` | Required. The views tool calls this `--sandbox_dataset_prefix`. |
| `--load_up_to_datasets` / `--load_up_to_addresses` | One is required. They scope only which views load (stages 2 and 7). |
| `--unattended` | Asks only the questions about what will run, then runs on its own. See the next section. |
| `--project_id` | Default `recidiviz-staging`. |
| `--raw_data_source_instance` | PRIMARY (default) or SECONDARY. |
| `--exports` | Also publishes to the staging product buckets. Needs two typed confirmations; a person runs it, never with `--unattended`. |

The pipelines do not depend on `--load_up_to_*`: every run launches the ingest
pipeline and every metric and supplemental pipeline for the state in
`recidiviz/pipelines/calculation_pipeline_templates.yaml`.

Stages, with the prompt each one shows (timings are rough, from a handful of
runs):

1. Pipeline summary table, then `Continue?  [y/n]:`.
2. View collection, before any pipeline: the rebase check (exit 1 if it
   fails), possibly the 24-hour warning, then `Continue with loading N views?
   [y/n]:`.
3. `Will upload Docker image with Dataflow pipeline code changes.` 2-5
   minutes on Cloud Build.
4. `Will create sandbox datasets for Dataflow pipelines output.` Deletes and
   recreates `{prefix}_*`.
5. `Will start the sandbox ingest Dataflow pipeline.` 5-60 minutes depending on
   the state, then `Has it completed? [y/n]:` in a loop.
6. `Will run the sandbox metric/supplemental Dataflow pipelines.` 5-30
   minutes each, launched together, then `Have they completed? [y/n]:`.
7. `Will load the [N] collected BigQuery views to a sandbox.` 10-60 minutes.

Each stage prompt from 3 on reads `Continue? [(P)roceed, (S)kip, or (E)xit]`.
Answering `S` at stage 3 reuses the last image built under your git
user.name, so only do it when that image was built from the same code.
Output datasets are `{prefix}_{dataset}`, e.g.
`agaidus100526nd_normalized_state`. The Dataflow jobs are named
`{prefix, lowercased, _ as -}-us-xx-ingest-test` and so on, in the state's
region from `DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE`.

## Unattended end-to-end: `--unattended`

With `--unattended`, a person answers stages 1 and 2 in the first few
minutes, and everything after runs on its own: stages 3-7 proceed without
asking, and stages 5 and 6 poll the jobs they launched instead of asking
"Has it completed?". Use it whenever Claude is asked to run an end-to-end
sandbox.

1. Confirm with the user: the worktree, the prefix, and the
   `--load_up_to_datasets`, naming every dataset the validation will read.
2. The run needs a terminal a person can type in, for the two up-front
   questions. In the Claude Code desktop app, start it in the Terminal panel
   (or have the person run it there) and read the panel to follow it;
   elsewhere, hand the person the command. Launch from the worktree that has
   the change:

   ```bash
   caffeinate -i uv run python -m recidiviz.tools.load_end_to_end_data_sandbox \
       --state_code US_XX --sandbox_prefix {prefix} \
       --load_up_to_datasets {datasets} --unattended
   ```

   A run in the person's own terminal outlives the Claude session.
   `caffeinate -i` prevents idle sleep, not sleep from closing the lid.
3. Before the person answers the view-count question, read the summary table
   and view list with them against the checklist in "How to know every
   relevant view loads", and say what you see. That question is the run's
   last chance to catch a view count far larger than intended.
4. Follow the run by the tool's own lines: `Submitted build in ...`,
   `Job {name} successfully launched`, `Job {name}: JOB_STATE_{STATE}` (once a
   minute on each change), `Will load the [N] collected BigQuery views`,
   `Error processing [...]`, and `[COMPLETE!]`. Where the terminal cannot be
   read, follow the jobs with `gcloud dataflow jobs list
   --project=recidiviz-staging --region={region}
   --filter="name~^{job_name_prefix}-"` and ask the person to paste the end of
   the output.
5. Post these updates, one short line each, and nothing in between:
   - Questions answered: the pipelines the run will launch, and "N views will
     load".
   - Image build done.
   - Each pipeline launched: the job name and the Dataflow console link,
     `https://console.cloud.google.com/dataflow/jobs?project={project}`.
   - Each stage Done, with how long it took.
   - View load started, and finished.
   - Finish. On `[COMPLETE!]`: the `{prefix}_*` datasets and when their tables
     expire (24 hours after creation). On an error: the message, which views
     failed (`Error processing [...]`), and the fix (see Troubleshooting).

Baseline alongside, when the user wants to measure what an ingest change did:
compare against an end-to-end sandbox built from the branch's merge base and
run at the same time, not against staging's
deployed tables, which may predate recent merges and were built from an older
raw data snapshot. The branch's own worktree is wherever the user already
works; the baseline needs a second one at the merge base, with its own venv:

```bash
git worktree add .claude/worktrees/{name}_base --detach "$(git merge-base {branch} origin/main)"
cd .claude/worktrees/{name}_base && uv sync --all-extras
```

A Claude session started inside a worktree cannot create another, so the user
runs this from the main checkout. Name the branch explicitly: `HEAD` there is
whatever the main checkout has checked out, not the branch under test. For a
branch stacked on another open PR, the baseline is that PR's branch in place
of the merge base, so the diff isolates the stacked change alone. Give the
baseline run a distinct prefix and its own image name (see "Launch order").
Remove the worktree when the sandboxes are deleted: `git worktree remove
.claude/worktrees/{name}_base`.

Launch order for more than one run:

- Two runs under one git user.name overwrite each other's image, and
  autoscaled workers pull whichever is there. Give a run from a second
  worktree its own image name by prefixing its command with
  `GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=user.name GIT_CONFIG_VALUE_0={your_git_user_name}base`.
- Runs that share an image name (same worktree and user.name) each rebuild it
  from the same code, so they do not conflict as long as nobody edits that
  worktree while they run.
- Do not start two runs at the same moment: each run's rebase check runs `git
  fetch --prune`, and two fetches against the same repository's refs can fail
  one run with `cannot lock ref`. Start the next run once the previous one has
  passed `Continue with loading N views?`, which comes after its fetch.

## Views off an end-to-end sandbox

After one end-to-end run, iterate on views without rerunning the pipelines by
pointing a views-only load at its outputs. Works until the run's tables
expire, 24 hours after each was created; the ingest outputs are written
first, so count from the ingest stage, not from when the run finished.
For state `US_XX` and end-to-end prefix `P`:

```bash
caffeinate -i uv run python -m recidiviz.tools.load_views_to_sandbox \
    --sandbox_dataset_prefix {new_prefix} --state_code_filter US_XX --prompt \
    --allow_slow_views --failure-mode FAIL_EXHAUSTIVELY \
    --input_source_table_dataset_overrides_json '{"us_xx_ingest_view_results": "P_us_xx_ingest_view_results", "us_xx_normalized_state": "P_us_xx_normalized_state", "us_xx_state": "P_us_xx_state", "dataflow_metrics": "P_dataflow_metrics", "supplemental_data": "P_supplemental_data"}' \
    auto --load_up_to_datasets {datasets}
```

- That map is the one the end-to-end tool uses itself
  (`get_view_update_input_dataset_overrides_dict`). Drop `supplemental_data`
  if the run's summary table showed no supplemental pipeline. Add
  `"us_xx_raw_data": "us_xx_raw_data_secondary"` only if the run used
  `--raw_data_source_instance SECONDARY`.
- Keys must be source-table datasets. `normalized_state` is not one (it is
  materialized from the `normalized_state_views` union views); override
  `us_xx_normalized_state` and the union views rebuild into
  `{new_prefix}_normalized_state`.
- Use `auto --load_up_to_*`, not `--load_changed_views_only`: every view that
  reads an overridden table counts as a start view, and
  `--load_changed_views_only` stops at the union views.
- `--state_code_filter` must match the run's state.
- To load whole datasets regardless of change detection, with no rebase
  check, use `manual --dataset_ids_to_load {datasets}` with the same overrides.

## Compare

Optional, for when the user wants to measure what a change did. Two sandboxes,
branch against baseline. The baseline is always
`--dataset_original` and the branch `--dataset_new`, whatever letters their
prefixes end in:

```bash
uv run python -m recidiviz.tools.calculator.compare_views \
    --project_id recidiviz-staging \
    --dataset_original {baseline_prefix}_{dataset} --dataset_new {branch_prefix}_{dataset} \
    --view_id {view}_materialized --output_dataset_prefix {branch_prefix}cmp \
    --grouping_columns state_code --primary_keys state_code,external_id
```

A quick check of a views-only change can compare the sandbox against the
deployed view instead (`--dataset_original {dataset} --dataset_new
{prefix}_{dataset}`), as long as the baseline rules below don't matter for
that view.

Output lands in
`{output_dataset_prefix}_{dataset_original}_comparison_output.{view}_[full|differences]`,
and summary stats print to the console. In the differences table each
non-key column is NULL where the two sides match and a `STRUCT<o, n>` where they differ, so
`COUNTIF(col IS NOT NULL)` counts the differences in one column. The script
ignores flags it does not know, so a misspelled flag silently does nothing.
Without `--primary_keys` the script guesses id-like columns;
opportunity record views key on `state_code,external_id`, `resident_record`
on `state_code,person_external_id`, and `sentence_status_raw_text_sessions`
on `state_code,person_id,sentence_id,start_date`. "One or more tables to
compare do not exist" means one side never loaded that view: descendants were
not loaded, or the 24-hour expiry hit. The comparison datasets,
`{output_dataset_prefix}_{dataset}_comparison_output`, are sandbox datasets
too and need the same cleanup.

Two baseline rules: build the baseline from the branch's merge base (see
"Baseline alongside" for stacked branches), not the tip of `origin/main`
(which drifts ahead) and not the deployed tables (only as fresh
as the last calc DAG run); and run both sides close together, since record
views read `CURRENT_DATE` and raw `_latest` views.

## Troubleshooting

| In the log | Cause | Fix |
|---|---|---|
| `Please run: gcloud auth login --update-adc` | ADC expired | Run it, rerun |
| At startup, before anything launched: `Cannot read Dataflow jobs in [...]` (plural), or `Permission 'dataflow.jobs.list' denied` | The account cannot read Dataflow jobs | Ask the Data Platform team for access that includes `dataflow.jobs.list` and `dataflow.jobs.get`, then rerun. Nothing launched, so nothing to clean up |
| Mid-run, naming one job: `Cannot read Dataflow job [{job name}] ...` | The account lost Dataflow read access during the run, for example when temporary access expired | The named job and any others launched keep running. Once access is back, cancel them in the Dataflow console, then rerun; a rerun with the same prefix recreates the datasets they write to |
| `Cannot find commit [...] You must rebase this branch` | Branch lacks the last view-update commit; staging may have deployed since the branch was cut | Rebase (pre-flight 4) |
| `cannot lock ref` after ``Command failed: `git fetch`` | Two runs' rebase-check fetches collided; worktrees share one repository's refs | Rerun; start a second run only after the first has passed `Continue with loading N views?` |
| `has not successfully completed in over 24 hours` | The last staging view deploy failed or is late | Expect extra "changed" views; the person answers it (up front under `--unattended`) |
| `Sandbox prefix '...' contains state code` | A `us_xx` token in the prefix does not match the filter | Rename the prefix or match `--state_code_filter` |
| `Not found: Table ..._materialized` | Ancestor not materialized in the sandbox | Manual load of the downstream view with `--update_ancestors True` |
| `DAG processing failed with the following errors` | One or more views failed; each is named in an `Error processing [...]` line | Each error is in the output above it |
| `404 Not found: Table {project}:{dataset}.{table}` on a source table (no `_materialized` suffix) | A view the load swept in reads a source table that does not exist in this project, often a view merged since the last deploy whose table exists only in prod | The other views loaded (`FAIL_EXHAUSTIVELY`). Rerun only the view stage off the sandbox's outputs (see "Views off an end-to-end sandbox") with `--changed_addresses_to_ignore {dataset}.{view}`, or add that flag to the next end-to-end run |
| `[BigQueryViewDagWalker Node Failure] ... took ... seconds` | A view passed the 6 minute limit; this fails even under FAIL_EXHAUSTIVELY | `--allow_slow_views` |
| `Permission denied while getting Drive credentials` | Views that read Google Sheets | `gcloud auth login --enable-gdrive-access --update-adc` |
| `Job launch failed..` (under `--unattended`, then `Dataflow refused to launch [...]`) | Dataflow refused the launch; the response follows | Fix the cause (quota, region, params) and rerun |
| `Dataflow job [...] ended in state [...]` | A pipeline failed or was cancelled | Open the console link in the message for the job's error logs |
| `Dataflow jobs [...] are still running after 6:00:00` | A stage passed `--unattended`'s 6 hour wait limit, most likely a stuck job | The jobs keep running. Check them in the Dataflow console, cancel any that are stuck, then rerun |
| ``Command timed out: `gcloud builds submit`` | The build passed the tool's 15 minute limit; Cloud Build continues to 30 | Wait for `gcloud builds list --ongoing` to empty, then rerun without `--unattended` and answer `S` at the image stage to reuse the image that build pushed |
| ``Command timed out: `gcloud dataflow flex-template build`` | The template upload passed the 15 second limit | Rerun |
| Jobs run someone else's code | Two concurrent runs under one `git user.name` | Give the second run its own image name (see "Launch order"), or start it after the first run's pipelines are Done |
| `One or more tables to compare do not exist` | A side never loaded the view | Load with descendants, within 24 hours |

## Cleanup

Tables expire after 24 hours; the datasets do not. Include the
`compare_views` output datasets, which share the prefix.

```bash
bq ls --project_id recidiviz-staging | grep {prefix}
bq rm -r -f recidiviz-staging:{dataset}
```

The end-to-end tool deletes and recreates the `{prefix}_*` Dataflow datasets
on a rerun.

## Related tools and docs

- `recidiviz.tools.load_raw_data_views_to_sandbox`: raw data views only.
- `recidiviz.tools.calculator.compare_dataflow_output_to_sandbox` and
  `compare_metric_view_output_to_sandbox`: Dataflow and metric view diffs.
- Each tool's `--help` (and `auto --help` / `manual --help` for the views
  tool's subcommands) and module docstring: the full flag list.
- `recidiviz/big_query/CLAUDE.md`: how views, sandbox contexts and
  materialization work.
- The Notion "load_views_to_sandbox DA Tips" pages: the walkthrough for people.
