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
source ${BASH_SOURCE_DIR}/../../script_base.sh
source ${BASH_SOURCE_DIR}/../deploy_helpers.sh


read -p "Project ID: " PROJECT_ID

reconfigure_terraform_backend $PROJECT_ID

DOCKER_TAG=$(terraform -chdir=${BASH_SOURCE_DIR}/terraform output -raw docker_image_tag)
GIT_HASH=$(terraform -chdir=${BASH_SOURCE_DIR}/terraform output -raw git_hash)

function tf-import {
  RESOURCE=$1
  ADDRESS=$2
  terraform import -config=recidiviz/tools/deploy/terraform \
  -var="project_id=$PROJECT_NAME" \
  -var="git_hash=$GIT_HASH" \
  -var="docker_image_tag=$DOCKER_TAG" \
  $RESOURCE $ADDRESS
}

if [[ $PROJECT_ID == "recidiviz-staging"]]; then

  # Remove jails_database resource and subresources
  terraform state rm module.jails_database.google_sql_database_instance.data
  terraform state rm module.jails_database.google_sql_user.postgres

  # Remove operations_database resource and subresources
#  terraform state rm module.operations_database.google_sql_database_instance.data
#  terraform state rm module.operations_database.google_sql_user.postgres

  # Remove state_database resource and subresources
#  terraform state rm module.state_database.google_sql_database_instance.data
#  terraform state rm module.state_database.google_sql_user.postgres

  # Import new resource jails_database
  tf-import "module.jails_database.google_sql_database_instance.data" projects/$PROJECT_ID/instances/dev-data-v2
  tf-import "module.jails_database.google_sql_user.postgres" projects/$PROJECT_ID/instances/dev-data-v2

  # Import new resource operations_database
#  tf-import "module.operations_database.google_sql_database_instance.data" projects/$PROJECT_ID/instances/dev-operations-data-v2
#  tf-import "module.operations_database.google_sql_user.postgres" projects/$PROJECT_ID/instances/dev-operations-data-v2

    # Import new resource state_database
#  tf-import "module.state_database.google_sql_database_instance.data" projects/$PROJECT_ID/instances/dev-state-data-v2
#  tf-import "module.state_database.google_sql_user.postgres" projects/$PROJECT_ID/instances/dev-state-data-v2

elif [[ $PROJECT_ID == "recidiviz-123" ]]; then
  continue
  # Remove jails_database resource and subresources
#  terraform state rm module.jails_database.google_sql_database_instance.data
#  terraform state rm module.jails_database.google_sql_user.postgres

  # Remove operations_database resource and subresources
#  terraform state rm module.operations_database.google_sql_database_instance.data
#  terraform state rm module.operations_database.google_sql_user.postgres

  # Remove state_database resource and subresources
#  terraform state rm module.state_database.google_sql_database_instance.data
#  terraform state rm module.state_database.google_sql_user.postgres

  # Import new resource jails_database
#  tf-import "module.jails_database.google_sql_database_instance.data" projects/$PROJECT_ID/instances/prod-data-v2
#  tf-import "module.jails_database.google_sql_user.postgres" projects/$PROJECT_ID/instances/prod-data-v2

  # Import new resource operations_database
#  tf-import "module.operations_database.google_sql_database_instance.data" projects/$PROJECT_ID/instances/prod-operations-data-v2
#  tf-import "module.operations_database.google_sql_user.postgres" projects/$PROJECT_ID/instances/prod-operations-data-v2

    # Import new resource state_database
#  tf-import "module.state_database.google_sql_database_instance.data" projects/$PROJECT_ID/instances/prod-state-data-v2
#  tf-import "module.state_database.google_sql_user.postgres" projects/$PROJECT_ID/instances/prod-state-data-v2
fi

# Secrets to update

# STAGING
# sqlalchemy_clousql_instance_id=recidiviz-staging:us-east4:dev-data-v2
# operations_clousql_instance_id=recidiviz-staging:us-east1:dev-operations-data-v2
# state_clousql_instance_id=recidiviz-staging:us-east1:dev-state-data-v2

# jails database
# sqlalchemy_db_client_cert
# sqlalchemy_db_client_key
# sqlalchemy_db_host
# sqlalchemy_db_name
# sqlalchemy_db_password
# sqlalchemy_db_server_cert
# sqlalchemy_db_user

# operations database
# operations_db_client_cert
# operations_db_client_key
# operations_db_host
# operations_db_name
# operations_db_password
# operations_db_server_cert
# operations_db_user

# state database
# state_db_client_cert
# state_db_client_key
# state_db_host
# state_db_name
# state_db_password
# state_db_server_cert
# state_db_user

# PRODUCTION
# sqlalchemy_clousql_instance_id=recidiviz-123:us-east4:dev-data-v2
# operations_clousql_instance_id=recidiviz-123:us-east1:dev-operations-data-v2
# state_clousql_instance_id=recidiviz-123:us-east1:dev-state-data-v2

# jails database
# sqlalchemy_db_client_cert
# sqlalchemy_db_client_key
# sqlalchemy_db_host
# sqlalchemy_db_name
# sqlalchemy_db_password
# sqlalchemy_db_server_cert
# sqlalchemy_db_user

# operations database
# operations_db_client_cert
# operations_db_client_key
# operations_db_host
# operations_db_name
# operations_db_password
# operations_db_server_cert
# operations_db_user

# state database
# state_db_client_cert
# state_db_client_key
# state_db_host
# state_db_name
# state_db_password
# state_db_server_cert
# state_db_user