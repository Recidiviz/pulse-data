#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../../script_base.sh
source ${BASH_SOURCE_DIR}/../deploy_helpers.sh

read -p "Project ID: " PROJECT_ID

reconfigure_terraform_backend $PROJECT_ID

DOCKER_TAG=$(terraform -chdir=${BASH_SOURCE_DIR}/terraform output -raw docker_image_tag)
GIT_HASH=$(terraform -chdir=${BASH_SOURCE_DIR}/terraform output -raw git_hash)

terraform -chdir=./recidiviz/tools/deploy/terraform import \
  -var=project_id=$PROJECT_ID \
  -var=docker_image_tag=$DOCKER_TAG \
  -var=git_hash=$GIT_HASH \
  module.state-aggregate-reports.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-state-aggregate-reports
