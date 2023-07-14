#!/usr/bin/env bash
# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
# Tool that deploys a test AppEngine image to a composer environment
# usage: ./recidiviz/tools/airflow/build_and_update_app_engine_image.sh
BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")

# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"


function check_active_gcloud_auth() {
  RESULT=$(gcloud auth list --format json | jq '.[].status | contains ("ACTIVE")')
  if [ "${RESULT}" != "true" ]; then
    echo_error "Must have an active gcloud login"
    exit 1
  fi
}


declare -a ENVIRONMENTS
read -r -a ENVIRONMENTS < <(gcloud composer environments list \
  --project=recidiviz-staging \
  --locations=us-central1 \
  --format json | jq -r .[].name | tr '\n' ' ' )

echo "Checking for active gcloud logins"
check_active_gcloud_auth

ENVIRONMENT=$(select_one 'Select an instance' "${ENVIRONMENTS[@]}")
IMAGE_NAME_SUFFIX=$(gcloud auth list --format json | jq -r '.[0].account' | cut -d '@' -f1)

IMAGE_NAME="us.gcr.io/recidiviz-staging/appengine/composer-gke-test-${IMAGE_NAME_SUFFIX}:latest"
echo "This will build image ${IMAGE_NAME} and update ${ENVIRONMENT}"
script_prompt "Do you wish to continue?"

run_cmd pipenv run docker-build -t "${IMAGE_NAME}"
run_cmd docker push "${IMAGE_NAME}"

CURRENT_IMAGE=$(
  gcloud composer environments describe "${ENVIRONMENT}" --format json | \
   jq -r '.config.softwareConfig.envVariables.RECIDIVIZ_APP_ENGINE_IMAGE'
)

if [ "${CURRENT_IMAGE}" == "${IMAGE_NAME}" ]; then
  echo "The environment is already configured to use ${IMAGE_NAME}"
  echo "KubernetesPodOperators will pull the latest image when run"
else
  echo "Updating environment to use ${IMAGE_NAME}"
  run_cmd gcloud composer environments update \
    --update-env-variables RECIDIVIZ_APP_ENGINE_IMAGE="${IMAGE_NAME}" \
    "${ENVIRONMENT}"
fi
