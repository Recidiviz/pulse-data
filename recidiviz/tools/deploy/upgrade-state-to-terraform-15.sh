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
read -p "Git hash: " GIT_HASH
read -p "Docker image tag: " DOCKER_IMAGE_TAG
script_prompt "Running for ${PROJECT_NAME}, revision ${GIT_HASH} @ docker image ${DOCKER_IMAGE_TAG} Continue?"

reconfigure_terraform_backend $PROJECT_NAME

echo "Using the following Terraform backend configuration:"
cat ${BASH_SOURCE_DIR}/terraform/.terraform/terraform.tfstate | jq -C .backend
script_prompt "Is this the correct backend to use?"


function tf-move {
  SOURCE=$1
  DESTINATION=$2

  terraform \
  -chdir=recidiviz/tools/deploy/terraform \
  state mv \
   $SOURCE \
   $DESTINATION
}

if [[ $PROJECT_NAME == "recidiviz-staging" ]]; then
  tf-move "module.case_triage_database.google_sql_user.readonly[\"readonly\"]" "module.case_triage_database.google_sql_user.readonly[0]"
elif [[ $PROJECT_NAME == "recidiviz-123" ]]; then
  tf-move "module.case_triage_database.google_sql_user.readonly[\"readonly\"]" "module.case_triage_database.google_sql_user.readonly[0]"
  tf-move "module.jails_database.google_sql_user.readonly[\"readonlyuser\"]" "module.jails_database.google_sql_user.readonly[0]"
  tf-move "module.justice_counts_database.google_sql_user.readonly[\"readonlyuser\"]" "module.justice_counts_database.google_sql_user.readonly[0]"
  tf-move "module.operations_database.google_sql_user.readonly[\"readonlyuser\"]" "module.operations_database.google_sql_user.readonly[0]"
  tf-move "module.state_database.google_sql_user.readonly[\"readonlyuser\"]" "module.state_database.google_sql_user.readonly[0]"
fi
