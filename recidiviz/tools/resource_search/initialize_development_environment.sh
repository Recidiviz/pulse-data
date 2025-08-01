#!/usr/bin/env bash

# run via ./recidiviz/tools/resource_search/initialize_development_environment.sh 

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"
export IS_DEV=true

function print_usage {
    echo_error "usage: $0 [-t]"
    echo_error "  -t: Write fake secrets instead of pulling from GSM. Used by GH Actions tests."
    run_cmd exit 1
}

function write_to_file {
  echo "\"$1\" > $2" | indent_output
  echo "$1" > "$2"
}

run_cmd mkdir -p recidiviz/local/gsm/

# Database secrets
write_to_file 'us_id' recidiviz/local/gsm/resource_search_db_host
write_to_file 'resource_user' recidiviz/local/gsm/resource_search_db_user
write_to_file 'example' recidiviz/local/gsm/resource_search_db_password
write_to_file '5440' recidiviz/local/gsm/resource_search_db_port
