#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../../script_base.sh

function write_to_file {
  echo "\"$1\" > $2" | indent_output
  echo $1 > $2
}

run_cmd mkdir -p recidiviz/local/gsm/

# Load staging Auth0 configuration. Uses subshell to remove additional output from gcloud util
# This secret is used by the Control Panel frontend to access the backend
AUTH0_CONFIGURATION=$(get_secret recidiviz-staging justice_counts_auth0)
write_to_file "$AUTH0_CONFIGURATION" recidiviz/local/gsm/justice_counts_auth0
# These secrets are used by the Admin Panel to access the Auth0 Management API and manage our users
AUTH0_DOMAIN=$(get_secret recidiviz-staging justice_counts_auth0_api_domain)
AUTH0_CLIENT_ID=$(get_secret recidiviz-staging justice_counts_auth0_api_client_id)
AUTH0_CLIENT_SECRET=$(get_secret recidiviz-staging justice_counts_auth0_api_client_secret)
write_to_file "$AUTH0_DOMAIN" recidiviz/local/gsm/justice_counts_auth0_api_domain
write_to_file "$AUTH0_CLIENT_ID" recidiviz/local/gsm/justice_counts_auth0_api_client_id
write_to_file "$AUTH0_CLIENT_SECRET" recidiviz/local/gsm/justice_counts_auth0_api_client_secret

# Load the Segment analytics public key so the Control Panel frontend can send analytics to the right destination
SEGMENT_KEY=$(echo $(gcloud secrets versions access latest --secret=justice_counts_segment_key --project recidiviz-staging))
write_to_file "$AUTH0_CONFIGURATION" recidiviz/local/gsm/justice_counts_segment_key

# Database secrets
write_to_file 'justice_counts' recidiviz/local/gsm/justice_counts_cloudsql_instance_id
write_to_file 'localhost' recidiviz/local/gsm/justice_counts_db_host
write_to_file 'justice_counts_user' recidiviz/local/gsm/justice_counts_db_user
write_to_file 'example' recidiviz/local/gsm/justice_counts_db_password
write_to_file '5432' recidiviz/local/gsm/justice_counts_db_port

write_to_file $(python -c 'import uuid; print(uuid.uuid4().hex)') recidiviz/local/gsm/justice_counts_secret_key
