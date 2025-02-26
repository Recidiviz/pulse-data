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

# These secrets are used to insert contact notes for TN
US_TN_INSERT_CONTACT_NOTE_URL=$(get_secret recidiviz-staging workflows_us_tn_insert_contact_note_url)
US_TN_INSERT_CONTACT_NOTE_KEY=$(get_secret recidiviz-staging workflows_us_tn_insert_contact_note_key)
write_to_file "$US_TN_INSERT_CONTACT_NOTE_URL" recidiviz/local/gsm/workflows_us_tn_insert_contact_note_url
write_to_file "$US_TN_INSERT_CONTACT_NOTE_KEY" recidiviz/local/gsm/workflows_us_tn_insert_contact_note_key
