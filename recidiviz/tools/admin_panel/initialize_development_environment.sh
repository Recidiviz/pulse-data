#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

function write_to_file {
  echo "\"$1\" > $2" | indent_output
  echo $1 > $2
}

# Database secrets
write_to_file 'operations' recidiviz/local/gsm/operations_v2_cloudsql_instance_id
write_to_file 'localhost' recidiviz/local/gsm/operations_v2_db_host
write_to_file 'operations_user' recidiviz/local/gsm/operations_v2_db_user
write_to_file 'example' recidiviz/local/gsm/operations_v2_db_password
write_to_file '5432' recidiviz/local/gsm/operations_v2_db_port


./${BASH_SOURCE_DIR}/../case_triage/initialize_development_environment.sh || exit_on_fail
./${BASH_SOURCE_DIR}/../justice_counts/control_panel/initialize_development_environment.sh || exit_on_fail
