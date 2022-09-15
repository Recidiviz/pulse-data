#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh
source ${BASH_SOURCE_DIR}/../postgres/script_helpers.sh

MIGRATIONS_HASH=$1
PROJECT_ID=$2
DATABASE_HOST=$CLOUDSQL_PROXY_HOST
DATABASE_PORT=$CLOUDSQL_PROXY_MIGRATION_PORT

echo "Running migrations against all databases in ${PROJECT_ID}..."

function run_migrations {
  SECRET_BASE_NAME=$1
  SCHEMA_TYPE=$2

  echo "Running migrations to head for ${SCHEMA_TYPE}..."
  python -m recidiviz.tools.migrations.run_migrations_to_head \
    --database $SCHEMA_TYPE \
    --project-id $PROJECT_ID \
    --confirm-hash $MIGRATIONS_HASH \
    --skip-db-name-check \
    --using-proxy
}

# run_migrations clauses have been explicitly duplicated for ease of disabling/enabling per project
if [[ "$PROJECT_ID" = 'recidiviz-123' ]]; then
  run_migrations state_v2 STATE
  run_migrations operations_v2 OPERATIONS
  run_migrations justice_counts JUSTICE_COUNTS
  run_migrations case_triage CASE_TRIAGE
  run_migrations pathways PATHWAYS
elif [[ "$PROJECT_ID" = 'recidiviz-staging' ]]; then
  run_migrations state_v2 STATE
  run_migrations operations_v2 OPERATIONS
  run_migrations justice_counts JUSTICE_COUNTS
  run_migrations case_triage CASE_TRIAGE
  run_migrations pathways PATHWAYS
else
  echo_error "Unrecognized project id for migration: ${PROJECT_ID}"
  exit 1
fi
