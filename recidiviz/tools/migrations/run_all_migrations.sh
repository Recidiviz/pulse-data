#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

echo 'Running migrations against all databases.'

MIGRATIONS_HASH=$1
PROJECT_ID=$2

run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database JAILS --project-id "$PROJECT_ID" --ssl-cert-path ~/prod_data_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database STATE --project-id "$PROJECT_ID" --ssl-cert-path ~/prod_state_data_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database OPERATIONS --project-id "$PROJECT_ID" --ssl-cert-path ~/prod_operations_data_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database JUSTICE_COUNTS --project-id "$PROJECT_ID" --ssl-cert-path ~/prod_justice_counts_data_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database CASE_TRIAGE --project-id "$PROJECT_ID" --ssl-cert-path ~/prod_case_triage_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
