#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh
source ${BASH_SOURCE_DIR}/../postgres/script_helpers.sh

MIGRATIONS_HASH=$1
PROJECT_ID=$2
DATABASE_HOST=$CLOUDSQL_PROXY_HOST
DATABASE_PORT=$CLOUDSQL_PROXY_MIGRATION_PORT

echo "Running migrations against all databases in ${PROJECT_ID}..."

function kill_all_proxies {
  run_cmd ${BASH_SOURCE_DIR}/../postgres/cloudsql_proxy_control.sh -q -p ${DATABASE_PORT}
}

trap kill_all_proxies EXIT

function migration_transaction {
  SECRET_BASE_NAME=$1
  SCHEMA_TYPE=$2
  DATABASE_CONNECTION_STRING=$(get_secret $PROJECT_ID ${SECRET_BASE_NAME}_cloudsql_instance_id)

  DATABASE_USER=$(get_secret $PROJECT_ID ${SECRET_BASE_NAME}_db_user)

  # Verify the proxy is ready to be run
  run_cmd ${BASH_SOURCE_DIR}/../postgres/cloudsql_proxy_control.sh -d -c $DATABASE_CONNECTION_STRING -p $DATABASE_PORT

  # Run the proxy
  run_cmd ${BASH_SOURCE_DIR}/../postgres/cloudsql_proxy_control.sh -c $DATABASE_CONNECTION_STRING -p $DATABASE_PORT

  wait_for_postgres $DATABASE_HOST $DATABASE_PORT postgres $DATABASE_USER

  echo "Running migrations to head for ${SCHEMA_TYPE}..."
  run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head \
    --database $SCHEMA_TYPE \
    --project-id $PROJECT_ID \
    --confirm-hash $MIGRATIONS_HASH \
    --skip-db-name-check \
    --using-proxy

  # Quit the proxy
  kill_all_proxies
}

echo "Preemptively killing any running Cloud SQL Proxies..."
kill_all_proxies

# migration_transaction clauses have been explicitly duplicated for ease of disabling/enabling per project
if [[ "$PROJECT_ID" = 'recidiviz-123' ]]; then
  migration_transaction state_v2 STATE
  migration_transaction operations_v2 OPERATIONS
  migration_transaction justice_counts JUSTICE_COUNTS
  migration_transaction case_triage CASE_TRIAGE
  migration_transaction pathways PATHWAYS
elif [[ "$PROJECT_ID" = 'recidiviz-staging' ]]; then
  migration_transaction state_v2 STATE
  migration_transaction operations_v2 OPERATIONS
  migration_transaction justice_counts JUSTICE_COUNTS
  migration_transaction case_triage CASE_TRIAGE
  migration_transaction pathways PATHWAYS
else
  echo_error "Unrecognized project id for migration: ${PROJECT_ID}"
  exit 1
fi
