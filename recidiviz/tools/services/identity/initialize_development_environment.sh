#!/usr/bin/env bash
# Initializes the development environment for the identity service.
BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../../script_base.sh"

function write_to_file {
  echo "\"$1\" > $2" | indent_output
  echo "$1" > "$2"
}

run_cmd mkdir -p recidiviz/local/gsm/

# Create development database secrets -- see recidiviz/utils/secrets.py
write_to_file 'identity_service' recidiviz/local/gsm/identity_service_cloudsql_instance_id
write_to_file 'identity_service_db' recidiviz/local/gsm/identity_service_db_host
write_to_file 'identity_service_user' recidiviz/local/gsm/identity_service_db_user
write_to_file 'example' recidiviz/local/gsm/identity_service_db_password
write_to_file '5441' recidiviz/local/gsm/identity_service_db_port
