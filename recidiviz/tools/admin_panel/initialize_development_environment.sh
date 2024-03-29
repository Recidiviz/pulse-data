#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"

function write_to_file {
  echo "\"$1\" > $2" | indent_output
  echo "$1" > "$2"
}

run_cmd mkdir -p recidiviz/local/gsm/

# Database secrets
write_to_file 'operations' recidiviz/local/gsm/operations_v2_cloudsql_instance_id
write_to_file 'localhost' recidiviz/local/gsm/operations_v2_db_host
write_to_file 'operations_user' recidiviz/local/gsm/operations_v2_db_user
write_to_file 'example' recidiviz/local/gsm/operations_v2_db_password
write_to_file '5432' recidiviz/local/gsm/operations_v2_db_port
write_to_file 'redis' recidiviz/local/gsm/admin_panel_redis_host
write_to_file '6379' recidiviz/local/gsm/admin_panel_redis_port


./"${BASH_SOURCE_DIR}/../case_triage/initialize_development_environment.sh" || exit_on_fail
./"${BASH_SOURCE_DIR}/../justice_counts/control_panel/initialize_development_environment.sh" || exit_on_fail
