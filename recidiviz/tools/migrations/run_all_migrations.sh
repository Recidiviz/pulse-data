#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

echo 'Running migrations against all databases.'

MIGRATIONS_HASH=$1
PROJECT_ID=$2

if [[ "$PROJECT_ID" = 'recidiviz-123' ]]; then
    run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database JAILS --project-id recidiviz-123 --ssl-cert-path ~/prod_data_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
    run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database STATE --project-id recidiviz-123 --ssl-cert-path ~/prod_state_data_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
    run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database OPERATIONS --project-id recidiviz-123 --ssl-cert-path ~/prod_operations_data_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
    run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database JUSTICE_COUNTS --project-id recidiviz-123 --ssl-cert-path ~/prod_justice_counts_data_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
    run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database CASE_TRIAGE --project-id recidiviz-123 --ssl-cert-path ~/prod_case_triage_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
elif [[ "$PROJECT_ID" = 'recidiviz-staging' ]]; then
    run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database JAILS --project-id recidiviz-staging --ssl-cert-path ~/dev_data_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
    run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database STATE --project-id recidiviz-staging --ssl-cert-path ~/dev_state_data_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
    run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database OPERATIONS --project-id recidiviz-staging --ssl-cert-path ~/dev_operations_data_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
    run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database JUSTICE_COUNTS --project-id recidiviz-staging --ssl-cert-path ~/dev_justice_counts_data_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
    run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database CASE_TRIAGE --project-id recidiviz-staging --ssl-cert-path ~/dev_case_triage_certs/ --skip-db-name-check --confirm-hash "$MIGRATIONS_HASH"
else
    echo_error "Unrecognized project id for migration: ${PROJECT_ID}"
    exit 1
fi
