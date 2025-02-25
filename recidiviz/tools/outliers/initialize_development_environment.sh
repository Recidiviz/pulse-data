#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"

function write_to_file {
  echo "\"$1\" > $2" | indent_output
  echo "$1" > "$2"
}

# Database secrets
write_to_file 'outliers' recidiviz/local/gsm/outliers_cloudsql_instance_id
write_to_file 'localhost' recidiviz/local/gsm/outliers_db_host
write_to_file 'outliers_user' recidiviz/local/gsm/outliers_db_user
write_to_file 'example' recidiviz/local/gsm/outliers_db_password
write_to_file '5432' recidiviz/local/gsm/outliers_db_port
