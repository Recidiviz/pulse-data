#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

echo 'Running prod migrations against all databases.'
script_prompt 'Are you running this script from prod-data-client?'

run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database JAILS --project-id recidiviz-123 --ssl-cert-path ~/prod_data_certs/
run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database STATE --project-id recidiviz-123 --ssl-cert-path ~/prod_state_data_certs/
run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database OPERATIONS --project-id recidiviz-123 --ssl-cert-path ~/prod_operations_data_certs/
run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database JUSTICE_COUNTS --project-id recidiviz-123 --ssl-cert-path ~/prod_justice_counts_data_certs/
run_cmd python -m recidiviz.tools.migrations.run_migrations_to_head --database CASE_TRIAGE --project-id recidiviz-123 --ssl-cert-path ~/prod_case_triage_certs/
