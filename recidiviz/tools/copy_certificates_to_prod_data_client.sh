#!/usr/bin/env bash
set -e

function scp_to_data_client {
  # Copies file contents over SCP to prod-data-client
  directory="/etc/ssl/$1"
  file=$2
  echo "Creating $directory and $file on prod-data-client"
  cat | gcloud compute ssh prod-data-client --project recidiviz-123 --zone="us-east4-c" --command="mkdir -p $directory && sudo cat > $directory/$file"
}

function copy_secret {
  project=$1
  secret=$2
  directory=$3
  file=$4

  echo "[$project] Downloading $secret secret data"

  secret_version_uri=$(gcloud secrets versions list $secret --project $project --filter=enabled --limit 1 --uri) || exit $?
  secret_data=$(gcloud secrets versions access $secret_version_uri) || exit $?

  echo "$secret_data" | scp_to_data_client $directory $file
}

# Development certificates
# Uncomment once certificates are provisioned
#copy_secret recidiviz-staging sqlalchemy_db_server_cert  "certs" "dev-server-ca.pem"
#copy_secret recidiviz-staging sqlalchemy_db_client_key   "private" "dev-client-key.pem"
#copy_secret recidiviz-staging sqlalchemy_db_client_cert  "certs" "dev-client-cert.pem"

# Uncomment once certificates are provisioned
#copy_secret recidiviz-staging justice_counts_db_server_cert  "certs" "dev-justice-counts-server-ca.pem"
#copy_secret recidiviz-staging justice_counts_db_client_cert  "certs" "dev-justice-counts-client-cert.pem"
#copy_secret recidiviz-staging justice_counts_db_client_key   "private" "dev-case-triage-client-key.pem"

copy_secret recidiviz-staging case_triage_db_server_cert "certs" "dev-case-triage-server-ca.pem"
copy_secret recidiviz-staging case_triage_db_client_key  "private" "dev-case-triage-client-key.pem"
copy_secret recidiviz-staging case_triage_db_client_cert "certs" "dev-case-triage-client-cert.pem"

# Uncomment once certificates are provisioned
#copy_secret recidiviz-staging operations_db_client_key   "private" "dev-case-operations-client-key.pem"
#copy_secret recidiviz-staging operations_db_server_cert  "certs" "dev-operations-server-ca.pem"
#copy_secret recidiviz-staging operations_db_client_cert  "certs" "dev-operations-client-cert.pem"

# Uncomment once certificates are provisioned
#copy_secret recidiviz-staging state_db_server_cert  "certs" "dev-state-server-ca.pem"
#copy_secret recidiviz-staging state_db_client_key   "certs" "dev-state-client-key.pem"
#copy_secret recidiviz-staging state_db_client_cert  "certs" "dev-state-client-cert.pem"

## Production certificates
#
#copy_secret recidiviz-123 sqlalchemy_db_server_cert  "certs"   "server-ca.pem"
#copy_secret recidiviz-123 sqlalchemy_db_client_cert  "certs"   "client-cert.pem"
#copy_secret recidiviz-123 sqlalchemy_db_client_key   "private" "client-key.pem"
#
#copy_secret recidiviz-123 justice_counts_db_server_cert  "certs"   "justice-counts-server-ca.pem"
#copy_secret recidiviz-123 justice_counts_db_client_cert  "certs"   "justice-counts-client-cert.pem"
#copy_secret recidiviz-123 justice_counts_db_client_key   "private" "justice-counts-client-key.pem"

# Uncomment once certificates are provisioned
#copy_secret recidiviz-123 case_triage_db_server_cert "certs"   "case-triage-server-ca.pem"
#copy_secret recidiviz-123 case_triage_db_client_cert "certs"   "case-triage-client-cert.pem"
#copy_secret recidiviz-123 case_triage_db_client_key  "private" "case-triage-client-key.pem"
#
#copy_secret recidiviz-123 operations_db_server_cert  "certs"   "operations-server-ca.pem"
#copy_secret recidiviz-123 operations_db_client_cert  "certs"   "operations-client-cert.pem"
#copy_secret recidiviz-123 operations_db_client_key   "private" "operations-client-key.pem"
#
#copy_secret recidiviz-123 state_db_server_cert  "certs"   "state-server-ca.pem"
#copy_secret recidiviz-123 state_db_client_cert  "certs"   "state-client-cert.pem"
#copy_secret recidiviz-123 state_db_client_key   "private" "state-client-key.pem"
