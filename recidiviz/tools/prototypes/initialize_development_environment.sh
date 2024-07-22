#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"

function write_to_file {
  echo "\"$1\" > $2" | indent_output
  echo "$1" > "$2"
}

# Load staging Dashboard Auth0 configuration. Uses subshell to remove additional output from gcloud util
DASHBOARD_AUTH0_CONFIGURATION=$(get_secret recidiviz-staging dashboard_auth0)
write_to_file "$DASHBOARD_AUTH0_CONFIGURATION" recidiviz/local/gsm/dashboard_auth0

# Load staging Dashboard Auth0 m2m configuration. Uses subshell to remove additional output from gcloud util
DASHBOARD_AUTH0_CONFIGURATION=$(get_secret recidiviz-staging dashboard_staging_auth0_m2m)
write_to_file "$DASHBOARD_AUTH0_CONFIGURATION" recidiviz/local/gsm/dashboard_staging_auth0_m2m

# Load staging Dashboard Auth0 m2m client secret. Uses subshell to remove additional output from gcloud util
DASHBOARD_AUTH0_CONFIGURATION=$(get_secret recidiviz-staging dashboard_staging_auth0_m2m_client_secret)
write_to_file "$DASHBOARD_AUTH0_CONFIGURATION" recidiviz/local/gsm/dashboard_staging_auth0_m2m_client_secret
