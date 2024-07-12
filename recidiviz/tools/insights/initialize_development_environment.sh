#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"

function write_to_file {
  echo "\"$1\" > $2" | indent_output
  echo "$1" > "$2"
}

# Database secrets
write_to_file 'insights' recidiviz/local/gsm/insights_cloudsql_instance_id
write_to_file 'insights_db' recidiviz/local/gsm/insights_db_host
write_to_file 'insights_user' recidiviz/local/gsm/insights_db_user
write_to_file 'example' recidiviz/local/gsm/insights_db_password
write_to_file '5432' recidiviz/local/gsm/insights_db_port
