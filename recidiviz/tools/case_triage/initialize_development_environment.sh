#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

function write_to_file {
  echo "\"$1\" > $2" | indent_output
  echo $1 > $2
}

read -p "Enter your email: " USER_EMAIL
run_cmd mkdir -p recidiviz/case_triage/local/gcs/case-triage-data/ recidiviz/local/gsm/

# Load staging Case Triage Auth0 configuration. Uses subshell to remove additional output from gcloud util
CASE_TRIAGE_AUTH0_CONFIGURATION=$(echo $(gcloud secrets versions access latest --secret=case_triage_auth0 --project recidiviz-staging))
write_to_file "$CASE_TRIAGE_AUTH0_CONFIGURATION" recidiviz/local/gsm/case_triage_auth0

# Load staging Dashboard Auth0 configuration. Uses subshell to remove additional output from gcloud util
DASHBOARD_AUTH0_CONFIGURATION=$(echo $(gcloud secrets versions access latest --secret=dashboard_auth0 --project recidiviz-staging))
write_to_file "$DASHBOARD_AUTH0_CONFIGURATION" recidiviz/local/gsm/dashboard_auth0

# Load staging INTERCOM_APP_KEY. Uses subshell to remove additional output from gcloud util
INTERCOM_APP_KEY=$(echo $(gcloud secrets versions access latest --secret=case_triage_intercom_app_key --project recidiviz-staging))
write_to_file "$INTERCOM_APP_KEY" recidiviz/local/gsm/case_triage_intercom_app_key

write_to_file $(python -c 'import uuid; print(uuid.uuid4().hex)') recidiviz/local/gsm/case_triage_secret_key

# References hostname specified in `services.case_triage_backend.links` from `docker-compose.case-triage.yml`
write_to_file 'rate_limit_cache' recidiviz/local/gsm/case_triage_rate_limiter_redis_host
write_to_file '6379' recidiviz/local/gsm/case_triage_rate_limiter_redis_port

# References hostname specified in `services.case_triage_backend.links` from `docker-compose.case-triage.yml`
write_to_file 'sessions_cache' recidiviz/local/gsm/case_triage_sessions_redis_host
write_to_file '6379' recidiviz/local/gsm/case_triage_sessions_redis_port

# References hostname specified in `services.case_triage_backend.links` from `docker-compose.case-triage.yml`
write_to_file 'pathways_metric_cache' recidiviz/local/gsm/pathways_metric_redis_host
write_to_file '6379' recidiviz/local/gsm/pathways_metric_redis_port


# Database secrets
write_to_file 'case_triage' recidiviz/local/gsm/case_triage_cloudsql_instance_id
write_to_file 'localhost' recidiviz/local/gsm/case_triage_db_host
write_to_file 'case_triage_user' recidiviz/local/gsm/case_triage_db_user
write_to_file 'example' recidiviz/local/gsm/case_triage_db_password
write_to_file '5432' recidiviz/local/gsm/case_triage_db_port

# Database secrets
write_to_file 'pathways' recidiviz/local/gsm/pathways_cloudsql_instance_id
write_to_file 'localhost' recidiviz/local/gsm/pathways_db_host
write_to_file 'pathways_user' recidiviz/local/gsm/pathways_db_user
write_to_file 'example' recidiviz/local/gsm/pathways_db_password
write_to_file '5432' recidiviz/local/gsm/pathways_db_port



# Set up application-specific configuration in GCS
write_to_file $(eval printf "%s" '[{\"email\": \"$USER_EMAIL\", \"is_admin\": true}]') recidiviz/case_triage/local/gcs/case-triage-data/allowlist_v2.json
write_to_file '{}' recidiviz/case_triage/local/gcs/case-triage-data/feature_variants.json
