#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")

# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"
# shellcheck source=recidiviz/tools/postgres/script_helpers.sh
source "${BASH_SOURCE_DIR}/../postgres/script_helpers.sh"

PROJECT_ID=$1

echo "Running migrations against all databases in ${PROJECT_ID}..."

function run_migrations {
  SCHEMA_TYPE=$1

  echo "Running migrations to head for ${SCHEMA_TYPE}..."
  run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head \
    --database "${SCHEMA_TYPE}" \
    --project-id "${PROJECT_ID}" \
    --skip-db-name-check
}

# run_migrations clauses have been explicitly duplicated for ease of disabling/enabling per project
if [[ "$PROJECT_ID" = 'recidiviz-123' ]]; then
  run_migrations OPERATIONS
  run_migrations JUSTICE_COUNTS
  run_migrations CASE_TRIAGE
  run_migrations PATHWAYS
  run_migrations WORKFLOWS
  run_migrations INSIGHTS
elif [[ "$PROJECT_ID" = 'recidiviz-staging' ]]; then
  run_migrations OPERATIONS
  run_migrations JUSTICE_COUNTS
  run_migrations CASE_TRIAGE
  run_migrations PATHWAYS
  run_migrations WORKFLOWS
  run_migrations INSIGHTS
else
  echo_error "Unrecognized project id for migration: ${PROJECT_ID}"
  exit 1
fi
