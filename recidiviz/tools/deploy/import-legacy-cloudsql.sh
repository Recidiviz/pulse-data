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

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh
source ${BASH_SOURCE_DIR}/deploy_helpers.sh

read -p "Project name: " PROJECT_NAME
read -p "Project number: " PROJECT_NUMBER
read -p "Git hash: " GIT_HASH
script_prompt "Running for ${PROJECT_NAME}/${PROJECT_NUMBER} @ revision ${GIT_HASH} Continue?"

reconfigure_terraform_backend $PROJECT_NAME

echo "Using the following Terraform backend configuration:"
cat .terraform/terraform.tfstate | jq -C .backend
script_prompt "Is this the correct backend to use?"


function tf-import {
  RESOURCE=$1
  ADDRESS=$2
  terraform import -config=recidiviz/tools/deploy/terraform -var="project_id=$PROJECT_NAME" -var="git_hash=$GIT_HASH" $RESOURCE $ADDRESS
}

if [[ $PROJECT_NAME == "recidiviz-staging" ]]; then
  # Jails database user differs between staging / production
  tf-import "module.jails_database.google_sql_user.postgres" $PROJECT_NAME/dev-data/postgres
  tf-import "module.jails_database.google_sql_database_instance.data" dev-data

  tf-import "module.justice_counts_database.google_sql_database_instance.data" dev-justice-counts-data
  tf-import "module.justice_counts_database.google_sql_user.postgres" $PROJECT_NAME/dev-justice-counts-data/postgres

  tf-import "module.operations_database.google_sql_database_instance.data" dev-operations-data
  tf-import "module.operations_database.google_sql_user.postgres" $PROJECT_NAME/dev-operations-data/postgres

  tf-import "module.state_database.google_sql_database_instance.data" dev-state-data
  tf-import "module.state_database.google_sql_user.postgres" $PROJECT_NAME/dev-state-data/postgres
elif [[ $PROJECT_NAME == "recidiviz-123" ]]; then
  # Imports the following terraform resources for each of our production databases:
  # - Client certificate retrieved via $(gcloud sql ssl client-certs list -i INSTANCE --uri)
  # - Client key secret data $(gcloud secrets versions access VERSION $(
  #     gcloud secrets versions list INSTANCE_db_client_key --filter=enabled --limit 1 --uri
  #   )
  # - Client certificate secret data $(gcloud secrets versions access VERSION $(
  #     gcloud secrets versions list INSTANCE_db_client_cert --filter=enabled --limit 1 --uri--uri
  #   )
  # - Server certificate secret via $(gcloud secrets list --uri)
  # - Server certificate secret data $(gcloud secrets versions access VERSION $(
  #     gcloud secrets versions list INSTANCE_db_server_cert --filter=enabled --limit 1 --uri
  #   )

  # Jails database
  # Import the `scraperwriter` user, as it matches what is contained in our `db_user` secrets
  tf-import "module.jails_database.google_sql_user.readonly[\"readonlyuser\"]" $PROJECT_NAME/prod-data/readonlyuser
  tf-import "module.jails_database.google_sql_user.postgres" $PROJECT_NAME/prod-data/scraperwriter
  tf-import "module.jails_database.google_sql_database_instance.data" prod-data
  tf-import \
    "module.jails_database.google_secret_manager_secret.secret_server_cert" \
    "https://secretmanager.googleapis.com/v1/projects/${PROJECT_NUMBER}/secrets/sqlalchemy_db_server_cert"
  tf-import \
    "module.jails_database.google_secret_manager_secret_version.secret_version_server_cert" \
    "projects/${PROJECT_NUMBER}/secrets/sqlalchemy_db_server_cert/versions/1"

  #Justice Counts database
  tf-import "module.justice_counts_database.google_sql_user.readonly[\"readonlyuser\"]" $PROJECT_NAME/prod-justice-counts-data/readonlyuser
  tf-import "module.justice_counts_database.google_sql_database_instance.data" prod-justice-counts-data
  tf-import "module.justice_counts_database.google_sql_user.postgres" $PROJECT_NAME/prod-justice-counts-data/postgres
  tf-import \
    "module.justice_counts_database.google_secret_manager_secret.secret_server_cert" \
    "https://secretmanager.googleapis.com/v1/projects/${PROJECT_NUMBER}/secrets/justice_counts_db_server_cert"
  tf-import \
    "module.justice_counts_database.google_secret_manager_secret_version.secret_version_server_cert" \
    "projects/${PROJECT_NUMBER}/secrets/justice_counts_db_server_cert/versions/1"

  # Operations database
  tf-import "module.operations_database.google_sql_database_instance.data" prod-operations-data
  tf-import "module.operations_database.google_sql_user.postgres" $PROJECT_NAME/prod-operations-data/postgres
  tf-import "module.operations_database.google_sql_user.readonly[\"readonlyuser\"]" $PROJECT_NAME/prod-operations-data/readonlyuser
  tf-import \
    "module.operations_database.google_secret_manager_secret.secret_server_cert" \
    "https://secretmanager.googleapis.com/v1/projects/${PROJECT_NUMBER}/secrets/operations_db_server_cert"
  tf-import \
    "module.operations_database.google_secret_manager_secret_version.secret_version_server_cert" \
    "projects/${PROJECT_NUMBER}/secrets/operations_db_server_cert/versions/1"

  # State database
  tf-import "module.state_database.google_sql_database_instance.data" prod-state-data
  tf-import "module.state_database.google_sql_user.postgres" $PROJECT_NAME/prod-state-data/postgres
  tf-import "module.state_database.google_sql_user.readonly[\"readonlyuser\"]" $PROJECT_NAME/prod-state-data/readonlyuser
  tf-import \
    "module.state_database.google_secret_manager_secret.secret_server_cert" \
    "https://secretmanager.googleapis.com/v1/projects/${PROJECT_NUMBER}/secrets/state_db_server_cert"
  tf-import \
    "module.state_database.google_secret_manager_secret_version.secret_version_server_cert" \
    "projects/${PROJECT_NUMBER}/secrets/state_db_server_cert/versions/1"
fi
