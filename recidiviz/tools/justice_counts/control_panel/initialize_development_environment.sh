#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../../script_base.sh

function write_to_file {
  echo "\"$1\" > recidiviz/justice_counts/control_panel/$2" | indent_output
  echo $1 > $2
}

# pushd stores the current directory for use by the popd command, 
# and then changes to the specified directory
pushd recidiviz/justice_counts/control_panel

run_cmd mkdir -p local/gsm/

# Load staging Auth0 configuration. Uses subshell to remove additional output from gcloud util
AUTH0_CONFIGURATION=$(echo $(gcloud secrets versions access latest --secret=justice_counts_auth0 --project recidiviz-staging))
write_to_file "$AUTH0_CONFIGURATION" local/gsm/justice_counts_auth0

write_to_file $(python -c 'import uuid; print(uuid.uuid4().hex)') local/gsm/justice_counts_secret_key

# Switch back to original directory
popd
