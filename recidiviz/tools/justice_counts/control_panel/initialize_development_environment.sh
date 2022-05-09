#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../../script_base.sh

function write_to_file {
  echo "\"$1\" > $2" | indent_output
  echo $1 > $2
}

run_cmd mkdir -p recidiviz/local/gsm/

# Load staging Auth0 configuration. Uses subshell to remove additional output from gcloud util
AUTH0_CONFIGURATION=$(echo $(gcloud secrets versions access latest --secret=justice_counts_auth0 --project recidiviz-staging))
write_to_file "$AUTH0_CONFIGURATION" recidiviz/local/gsm/justice_counts_auth0

write_to_file $(python -c 'import uuid; print(uuid.uuid4().hex)') recidiviz/local/gsm/justice_counts_secret_key
