#!/usr/bin/env bash
#
# Helpers for writing oneoff scripts to import resources to Terraform.
# Example script:
# #!/usr/bin/env bash
# BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
# source ${BASH_SOURCE_DIR}/terraform_oneoffs.sh
# terraform_import module.direct-ingest-state-storage.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-direct-ingest-state-storage

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../../script_base.sh
source ${BASH_SOURCE_DIR}/../deploy_helpers.sh

read -p "Project ID: " PROJECT_ID

# Terraform determines certain resources by looking at the directory structure,
# so give our shell the ability to open plenty of file descriptors.
ulimit -n 1024 || exit_on_fail

reconfigure_terraform_backend $PROJECT_ID

DOCKER_TAG=$(terraform -chdir=${BASH_SOURCE_DIR}/terraform output -raw docker_image_tag)
GIT_HASH=$(terraform -chdir=${BASH_SOURCE_DIR}/terraform output -raw git_hash)

function terraform_import {
    RESOURCE_ADDR=$1
    RESOURCE_ID=$2

    terraform -chdir=./recidiviz/tools/deploy/terraform import \
    -var=project_id=$PROJECT_ID \
    -var=docker_image_tag=$DOCKER_TAG \
    -var=git_hash=$GIT_HASH \
    $RESOURCE_ADDR $RESOURCE_ID
}

function terraform_mv {
  OLD_RESOURCE_ADDR=$1
  UPDATED_RESOURCE_ADDR=$2
  terraform -chdir=./recidiviz/tools/deploy/terraform state mv \
    "$OLD_RESOURCE_ADDR" "$UPDATED_RESOURCE_ADDR"
}
