#!/bin/sh
# Triggers an Airflow DAG via Cloud Composer with a stable, deterministic run ID
# to prevent duplicate DAG runs from Cloud Scheduler's at-least-once delivery.
#
# The run ID is constructed as "{schedule_id}--{YYYY-MM-DDTHH}" (UTC) where the
# hour is rounded to the nearest hour rather than truncated. This handles two
# things at once:
#   - Cloud Scheduler retries (which fire seconds after the original) land in
#     the same bucket, so Airflow rejects them as duplicates.
#   - Cloud Scheduler occasionally fires a few seconds early or late (10:59 or
#     11:01 for a "0 * * * *" cron); rounding to nearest puts both in the
#     11-bucket where the on-time fire would also land.
# Hour granularity (vs day) is required because some schedule_ids fire multiple
# times per day (e.g. an `hourly` schedule); a day-granularity run_id would
# collide on the second and subsequent fires.
#
# If a duplicate is detected, the script exits 0 so the Cloud Run job does not
# register as failed.
#
# Usage:
#   trigger_dag.sh \
#     --project <gcp_project_id> \
#     --composer-env <environment> \
#     --composer-location <location> \
#     --dag-id <dag_id> \
#     --config <json>
#
# Required environment variable:
#   SCHEDULE_ID - A human-readable identifier for the cron schedule that triggered
#     this run (e.g. "weekday-3am-pt"). Injected at runtime by Cloud Scheduler via
#     Cloud Run v2 container overrides — each scheduler job POSTs a JSON body with
#     overrides.containerOverrides[].env to set this value. See:
#     https://cloud.google.com/run/docs/reference/rest/v2/projects.locations.jobs/run

set -eu

while [ $# -gt 0 ]; do
  case "$1" in
    --project)            PROJECT="$2"; shift 2;;
    --composer-env)       COMPOSER_ENVIRONMENT="$2"; shift 2;;
    --composer-location)  COMPOSER_LOCATION="$2"; shift 2;;
    --dag-id)             DAG_ID="$2"; shift 2;;
    --config)             CONFIG_JSON="$2"; shift 2;;
    *) echo "Unknown flag: $1" >&2; exit 1;;
  esac
done

if [ -z "${SCHEDULE_ID:-}" ]; then
  echo "ERROR: SCHEDULE_ID environment variable is not set." >&2
  echo "This variable should be injected by Cloud Scheduler via Cloud Run v2" >&2
  echo "container overrides. If running manually, set it to a unique identifier" >&2
  echo "for this trigger (e.g. SCHEDULE_ID=manual-test)." >&2
  exit 1
fi

# Round to the nearest hour by adding 30 minutes then truncating: anything
# with minute >= 30 rolls up, anything with minute < 30 stays. Requires GNU
# `date -d` (present in the google/cloud-sdk:stable container).
RUN_ID="${SCHEDULE_ID}--$(date -u -d '+30 minutes' +%Y-%m-%dT%H)"
echo "Triggering ${DAG_ID} with run_id=${RUN_ID}"

OUTPUT=$(gcloud composer environments run "${COMPOSER_ENVIRONMENT}" \
  --project "${PROJECT}" \
  --location "${COMPOSER_LOCATION}" \
  dags trigger -- "${DAG_ID}" \
  --run-id "${RUN_ID}" \
  -c "${CONFIG_JSON}" 2>&1) && EXIT_CODE=0 || EXIT_CODE=$?

echo "${OUTPUT}"
echo "gcloud exit code: ${EXIT_CODE}"

if [ "${EXIT_CODE}" -ne 0 ]; then
  # Cloud Scheduler guarantees at-least-once delivery, so duplicate triggers are
  # expected on occasion. Exit 0 on duplicates so the Cloud Run job succeeds and
  # we don't generate spurious PagerDuty alerts.
  if echo "${OUTPUT}" | grep -qi 'already exists\|DagRunAlreadyExists'; then
    echo "Duplicate run detected (run_id=${RUN_ID}), exiting cleanly"
    exit 0
  fi
  exit "${EXIT_CODE}"
fi
