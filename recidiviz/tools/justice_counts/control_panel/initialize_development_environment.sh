#!/usr/bin/env bash

# run via ./recidiviz/tools/justice_counts/control_panel/initialize_development_environment.sh 

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../../script_base.sh"

function print_usage {
    echo_error "usage: $0 [-t]"
    echo_error "  -t: Write fake secrets instead of pulling from GSM. Used by GH Actions tests."
    run_cmd exit 1
}

is_ci=false
while getopts "t" flag; do
    case "${flag}" in
        t) is_ci=true;;
        *) print_usage
    esac
done

function write_to_file {
  echo "\"$1\" > $2" | indent_output
  echo "$1" > "$2"
}

run_cmd mkdir -p recidiviz/local/gsm/

# Database secrets
write_to_file 'justice_counts' recidiviz/local/gsm/justice_counts_v2_cloudsql_instance_id
write_to_file 'localhost' recidiviz/local/gsm/justice_counts_v2_db_host
write_to_file 'justice_counts_user' recidiviz/local/gsm/justice_counts_v2_db_user
write_to_file 'example' recidiviz/local/gsm/justice_counts_v2_db_password
write_to_file '5432' recidiviz/local/gsm/justice_counts_v2_db_port

# Do not fetch auth secrets if running from the CI pipeline.
if [[ ! $is_ci ]]
then
  # Load staging Auth0 configuration. Uses subshell to remove additional output from gcloud util
  # This secret is used by the Control Panel frontend to access the backend
  AUTH0_CONFIGURATION=$(get_secret justice-counts-staging justice_counts_auth0)
  write_to_file "$AUTH0_CONFIGURATION" recidiviz/local/gsm/justice_counts_auth0

  # These secrets are used by the Admin Panel to access the Auth0 Management API and manage our users
  AUTH0_DOMAIN=$(get_secret justice-counts-staging justice_counts_auth0_api_domain)
  AUTH0_CLIENT_ID=$(get_secret justice-counts-staging justice_counts_auth0_api_client_id)
  AUTH0_CLIENT_SECRET=$(get_secret justice-counts-staging justice_counts_auth0_api_client_secret)
  write_to_file "$AUTH0_DOMAIN" recidiviz/local/gsm/justice_counts_auth0_api_domain
  write_to_file "$AUTH0_CLIENT_ID" recidiviz/local/gsm/justice_counts_auth0_api_client_id
  write_to_file "$AUTH0_CLIENT_SECRET" recidiviz/local/gsm/justice_counts_auth0_api_client_secret

  # These secrets are used by the `request_api` script which allows us to mock requests to our backend API
  AUTH0_M2M_CLIENT_ID=$(get_secret justice-counts-staging justice_counts_auth0_m2m_client_id)
  AUTH0_M2M_CLIENT_SECRET=$(get_secret justice-counts-staging justice_counts_auth0_m2m_client_secret)
  write_to_file "$AUTH0_M2M_CLIENT_ID" recidiviz/local/gsm/justice_counts_auth0_m2m_client_id
  write_to_file "$AUTH0_M2M_CLIENT_SECRET" recidiviz/local/gsm/justice_counts_auth0_m2m_client_secret

  # Load the Segment analytics public key so the Control Panel frontend can send analytics to the right destination
  SEGMENT_KEY=$(get_secret justice-counts-staging justice_counts_segment_key)
  write_to_file "$SEGMENT_KEY" recidiviz/local/gsm/justice_counts_segment_key

  write_to_file "$(python -c 'import uuid; print(uuid.uuid4().hex)')" recidiviz/local/gsm/justice_counts_secret_key
fi
