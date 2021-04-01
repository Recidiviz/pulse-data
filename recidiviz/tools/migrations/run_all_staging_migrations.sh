#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

echo 'Running staging migrations against all databases.'

run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database JAILS --project-id recidiviz-staging --ssl-cert-path ~/dev_data_certs/
run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database STATE --project-id recidiviz-staging --ssl-cert-path ~/dev_state_data_certs/
run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database OPERATIONS --project-id recidiviz-staging --ssl-cert-path ~/dev_operations_data_certs/
run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database JUSTICE_COUNTS --project-id recidiviz-staging --ssl-cert-path ~/dev_justice_counts_data_certs/
run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database CASE_TRIAGE --project-id recidiviz-staging --ssl-cert-path ~/dev_case_triage_certs/
