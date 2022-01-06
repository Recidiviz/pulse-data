#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

function write_to_file {
  echo "\"$1\" > recidiviz/case_triage/$2" | indent_output
  echo $1 > $2
}

read -p "Enter your email: " USER_EMAIL
pushd recidiviz/case_triage
run_cmd mkdir -p local/gcs/case-triage-data/ local/gsm/

# Load staging Auth0 configuration. Uses subshell to remove additional output from gcloud util
AUTH0_CONFIGURATION=$(echo $(gcloud secrets versions access latest --secret=case_triage_auth0 --project recidiviz-staging))
write_to_file "$AUTH0_CONFIGURATION" local/gsm/case_triage_auth0

# Load staging INTERCOM_APP_KEY. Uses subshell to remove additional output from gcloud util
INTERCOM_APP_KEY=$(echo $(gcloud secrets versions access latest --secret=case_triage_intercom_app_key --project recidiviz-staging))
write_to_file "$INTERCOM_APP_KEY" local/gsm/case_triage_intercom_app_key

write_to_file $(python -c 'import uuid; print(uuid.uuid4().hex)') local/gsm/case_triage_secret_key

# References hostname specified in `services.case_triage_backend.links` from `docker-compose.yml`
write_to_file 'rate_limit_cache' local/gsm/case_triage_rate_limiter_redis_host
write_to_file '6379' local/gsm/case_triage_rate_limiter_redis_port

# References hostname specified in `services.case_triage_backend.links` from `docker-compose.yml`
write_to_file 'sessions_cache' local/gsm/case_triage_sessions_redis_host
write_to_file '6379' local/gsm/case_triage_sessions_redis_port

# Set up application-specific configuraiton
write_to_file $(eval printf "%s" '[{\"email\": \"$USER_EMAIL\", \"is_admin\": true}]') local/gcs/case-triage-data/allowlist_v2.json
write_to_file '{}' local/gcs/case-triage-data/feature_variants.json
popd
