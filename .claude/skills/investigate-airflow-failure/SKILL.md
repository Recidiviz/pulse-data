---
name: investigate-airflow-failure
description: Diagnose a failed Airflow task running on Cloud Composer by pulling task logs from GCS, identifying the first real error in the traceback, and inspecting the task's source and recent git history. Use when the user wants to investigate an Airflow task failure (e.g. "why did X task fail", "investigate the failing generate_airflow_dag_run_history task", "dig into the airflow failure in prod"). Can be invoked directly or as a specialist from the investigate-pd-incident skill.
---

# Skill: Investigate Airflow Failure

## Overview

Diagnose a specific failed Airflow task instance running on the Recidiviz Cloud
Composer environment. This is diagnose-only: surface the root cause and
relevant context. **Do not** propose code fixes, open PRs, retry the task, or
take any action on the Composer environment without the user explicitly asking.

## Inputs

You need three things to diagnose a failure:

- `dag_id` — the Airflow DAG id. Always includes a project prefix:
  `recidiviz-123_*` means prod, `recidiviz-staging_*` means staging.
- `task_id` — the Airflow task id (the `task_id` parameter of the Operator)
- `run_start` — the datetime the failing DAG run started, UTC

If any of these are missing, **ASK the user** — do not guess.

When invoked from `investigate-pd-incident`, all three come from the parsed
incident title.

## Constants

- Composer environment name: `orchestration-v2`
- Composer region: `us-central1`
- Prod GCP project: `recidiviz-123`
- Staging GCP project: `recidiviz-staging`

Source: `recidiviz/tools/deploy/terraform/cloud-composer.tf`. Only one Composer
env exists per project.

## Step 1: Derive the project

Take the first token of `dag_id` up to the first `_` as the project id:

- `recidiviz-123_hourly_monitoring_dag` → project `recidiviz-123` (prod)
- `recidiviz-staging_calculation_dag` → project `recidiviz-staging` (staging)

If the prefix doesn't match either, ask the user which project to look in.

## Step 2: Find the exact failed run_id

A DAG run's `run_id` looks like `manual__<ISO8601 timestamp>+00:00`, and the
timestamp is *not* always exactly equal to the `run_start` from the PD title
(there can be seconds of drift). Find the precise `run_id` from the Cloud
Logging scheduler record:

```bash
gcloud logging read '
resource.type="cloud_composer_environment"
resource.labels.environment_name="orchestration-v2"
"<TASK_ID>"
textPayload:"state=failed"
timestamp>="<RUN_START - 30m>"
timestamp<="<RUN_START + 4h>"
' --project=<PROJECT_ID> --limit=5 \
  --format='value(timestamp,textPayload)'
```

The text payload looks like:

> `TaskInstance Finished: dag_id=<dag>, task_id=<task>, run_id=manual__2026-04-20T16:01:59+00:00, ..., state=failed, ..., try_number=1, max_tries=0, ...`

Extract `run_id` and `try_number` from the match closest to `run_start`. If
there are multiple failed instances of the same task within the window, ask the
user which one to investigate.

## Step 3: Look up the Composer GCS bucket

```bash
gcloud composer environments describe orchestration-v2 \
  --location=us-central1 --project=<PROJECT_ID> \
  --format='value(config.dagGcsPrefix)'
```

Returns something like `gs://us-central1-orchestration-v-78d60713-bucket/dags`.
The logs live in the **same bucket, parallel prefix**: swap `/dags` for
`/logs`. Cache the bucket name after the first lookup in a session.

## Step 4: Fetch the failing task log

Log path format (note the literal `:` and `+` in `run_id=` — do **not**
URL-encode):

```
gs://<bucket>/logs/dag_id=<DAG_ID>/run_id=<RUN_ID>/task_id=<TASK_ID>/attempt=<TRY_NUMBER>.log
```

```bash
gsutil cat 'gs://<bucket>/logs/dag_id=<DAG_ID>/run_id=<RUN_ID>/task_id=<TASK_ID>/attempt=<TRY_NUMBER>.log' \
  | head -500
```

If the object is missing, check the parent directory with `gsutil ls` — the
DAG or task may have been renamed, or the try number may be wrong.

## Step 5: Extract the first real error

Task logs can be enormous and repetitive. Common noise pattern: BigQuery
streaming-insert failures produce a `reason: 'stopped'` line for every other
row in the failed batch (hundreds of lines of empty-message noise following the
actual error). Always grep for the first real signal:

```bash
gsutil cat '<LOG_PATH>' | grep -n -E "Traceback|ERROR|Exception|reason.*(invalid|schema|out of range|bad|notFound|accessDenied)" | head -20
```

Then read a ~35-line window starting around the first `Traceback` or
`ERROR - Task failed with exception` line — that contains the exception class,
full traceback (including a `/home/airflow/gcs/dags/recidiviz/...` source
path pointing at the call site), and the root cause message.

## Step 6: Read the task's source

The traceback's innermost Recidiviz frame points at
`/home/airflow/gcs/dags/recidiviz/...` which maps 1:1 to `recidiviz/...` in
this repo. Read the surrounding function with the `Read` tool to understand
what the task does.

If the failure is in the PythonOperator wrapper layer, find the task's
registration in its `*_dag.py` file (e.g. `monitoring_dag.py`,
`calculation_dag.py`) to locate the `python_callable`.

## Step 7: Check recent history on the task's source file

```bash
git log --oneline -n 15 -- <path/to/task_file.py>
```

Flag any commit in the last ~14 days that touches the code path implicated by
the traceback — that's the likeliest regression source. Note: recency alone
doesn't mean guilt; read the diffs before asserting causation.

## Step 8: Present the diagnosis

Structure your output in this order:

1. **TLDR** — 2–3 sentences in plain English: what failed, where, and the
   best guess at why. No jargon, no file paths.
2. **Task** — DAG, task id, run id, project, link to Airflow UI (get it with
   `gcloud composer environments describe ... --format='value(config.airflowUri)'`
   once per session, then append `/dags/<DAG_ID>/grid?dag_run_id=<RUN_ID>`)
3. **Error excerpt** — code block with the traceback header, the innermost
   Recidiviz frame, and the exception message. Trim repetitive noise.
4. **Source** — quote ≤ 10 lines of the implicated function with a
   `file_path:line` reference
5. **Recent changes** — output of `git log --oneline` on the file, with a
   one-line note per commit that looks potentially related
6. **Suggested next steps** — e.g. "file a bug", "retry once we understand
   the root cause", "check if the upstream data source changed". Keep this
   advisory; do not take any of these actions unsupervised.

## Gotchas

- **run_id ≠ run_start**: the PD incident subject has the *run start
  timestamp*, which is close to but not exactly the `run_id` timestamp. Use
  the scheduler logs in Step 2 to find the exact `run_id`.
- **Log names in Cloud Logging don't include task output**: `airflow-scheduler`
  and `airflow-worker` records describe task *lifecycle*, not the task's
  Python stdout/stderr. The real task log only lives in GCS.
- **try_number is usually 1**: for tasks with `max_tries=0` (no retries) the
  only attempt is `attempt=1.log`. If a task does retry, there can be multiple
  attempt files — diagnose the *last* one.
- **State codes US_ID / US_IX**: if the failing task touches Idaho data,
  confirm with the user which state code applies.
- **Never query ME or CA data** during investigation (CLAUDE.md rule).

## Related Documentation

- [Investigate PagerDuty Incident](../investigate-pd-incident/SKILL.md) — the
  router that dispatches here for Airflow alerts
- [cloud-composer.tf](../../../recidiviz/tools/deploy/terraform/cloud-composer.tf) —
  Composer environment config (env name, region, project logic)
- [monitoring_dag.py](../../../recidiviz/airflow/dags/monitoring_dag.py) —
  example DAG file showing the PythonOperator → python_callable pattern
