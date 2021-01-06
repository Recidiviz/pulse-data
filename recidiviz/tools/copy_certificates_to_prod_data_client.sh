#!/usr/bin/env bash
# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================

set -e

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/script_base.sh

read -p "Staging project name: " STAGING_PROJECT
read -p "Production project name: " PRODUCTION_PROJECT
script_prompt "Importing secrets for staging[${STAGING_PROJECT}] and production[${PRODUCTION_PROJECT}]. Continue?"

function scp_to_data_client() {
  # Copies file contents over SCP to prod-data-client
  directory="/etc/ssl/$1"
  file=$2
  echo "Creating $directory and $file on prod-data-client"
  cat | gcloud compute ssh prod-data-client \
    --project $PRODUCTION_PROJECT \
    --zone="us-east4-c" \
    --command="cat | sudo tee -a $directory/$file > /dev/null"
}

function copy_secret() {
  project=$1
  secret=$2
  directory=$3
  file=$4

  echo "[$project] Downloading $secret secret data"

  secret_version_uri=$(gcloud secrets versions list $secret --project $project --filter=enabled --limit 1 --uri) || exit $?
  secret_data=$(gcloud secrets versions access $secret_version_uri) || exit $?

  echo "$secret_data" | scp_to_data_client $directory $file
}

# Case Triage certs:
copy_secret $STAGING_PROJECT case_triage_db_client_key "private" "dev-case-triage-client-key.pem"
copy_secret $STAGING_PROJECT case_triage_db_server_cert "certs" "dev-case-triage-server-ca.pem"
copy_secret $STAGING_PROJECT case_triage_db_client_cert "certs" "dev-case-triage-client-cert.pem"

# TODO(#5202) Uncomment once production certificates are provisioned
#copy_secret $PRODUCTION_PROJECT case_triage_db_client_key  "private" "case-triage-client-key.pem"
#copy_secret $PRODUCTION_PROJECT case_triage_db_server_cert "certs"   "case-triage-server-ca.pem"
#copy_secret $PRODUCTION_PROJECT case_triage_db_client_cert "certs"   "case-triage-client-cert.pem"

# Jails certs:
copy_secret $STAGING_PROJECT sqlalchemy_db_client_key "private" "dev-client-key.pem"
copy_secret $STAGING_PROJECT sqlalchemy_db_server_cert "certs" "dev-server-ca.pem"
copy_secret $STAGING_PROJECT sqlalchemy_db_client_cert "certs" "dev-client-cert.pem"

# TODO(#5202) Uncomment once production certificates are provisioned
#copy_secret $PRODUCTION_PROJECT sqlalchemy_db_client_key   "private" "client-key.pem"
#copy_secret $PRODUCTION_PROJECT sqlalchemy_db_server_cert  "certs"   "server-ca.pem"
#copy_secret $PRODUCTION_PROJECT sqlalchemy_db_client_cert  "certs"   "client-cert.pem"


# Justice Counts certs:
copy_secret $STAGING_PROJECT justice_counts_db_client_key   "private" "dev-justice-counts-client-key.pem"
copy_secret $STAGING_PROJECT justice_counts_db_server_cert  "certs" "dev-justice-counts-server-ca.pem"
copy_secret $STAGING_PROJECT justice_counts_db_client_cert  "certs" "dev-justice-counts-client-cert.pem"

# TODO(#5202) Uncomment once production certificates are provisioned
#copy_secret $PRODUCTION_PROJECT justice_counts_db_client_key   "private" "justice-counts-client-key.pem"
#copy_secret $PRODUCTION_PROJECT justice_counts_db_server_cert  "certs"   "justice-counts-server-ca.pem"
#copy_secret $PRODUCTION_PROJECT justice_counts_db_client_cert  "certs"   "justice-counts-client-cert.pem"


# Operations certs:
copy_secret $STAGING_PROJECT operations_db_client_key   "private" "dev-operations-client-key.pem"
copy_secret $STAGING_PROJECT operations_db_server_cert  "certs" "dev-operations-server-ca.pem"
copy_secret $STAGING_PROJECT operations_db_client_cert  "certs" "dev-operations-client-cert.pem"

# TODO(#5202) Uncomment once production certificates are provisioned
#copy_secret $PRODUCTION_PROJECT operations_db_client_key   "private" "operations-client-key.pem"
#copy_secret $PRODUCTION_PROJECT operations_db_server_cert  "certs"   "operations-server-ca.pem"
#copy_secret $PRODUCTION_PROJECT operations_db_client_cert  "certs"   "operations-client-cert.pem"


# State certs:
copy_secret $STAGING_PROJECT state_db_client_key   "private" "dev-state-client-key.pem"
copy_secret $STAGING_PROJECT state_db_server_cert  "certs" "dev-state-server-ca.pem"
copy_secret $STAGING_PROJECT state_db_client_cert  "certs" "dev-state-client-cert.pem"

# TODO(#5202) Uncomment once production certificates are provisioned
#copy_secret $PRODUCTION_PROJECT state_db_client_key   "private" "state-client-key.pem"
#copy_secret $PRODUCTION_PROJECT state_db_server_cert  "certs"   "state-server-ca.pem"
#copy_secret $PRODUCTION_PROJECT state_db_client_cert  "certs"   "state-client-cert.pem"
