#!/usr/bin/env bash
#
# Helpers for writing oneoff scripts to import resources to Terraform.
# 
# Example script:
# #!/usr/bin/env bash
# BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# source "${BASH_SOURCE_DIR}/terraform_oneoffs.sh"
# terraform_import module.direct-ingest-state-storage.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-direct-ingest-state-storage
# 
# To test the import script, copy the existing state file into a subdirectory and run the script
# against that state file. Example:
# $ gsutil cp gs://recidiviz-staging-tf-state/default.tfstate gs://recidiviz-staging-tf-state/dev-danawillow/default.tfstate
# $ ./recidiviz/tools/deploy/oneoffs/import_external_ip_address.sh (Project ID: recidiviz-staging, TF state file prefix: dev-danawillow)
# $ ./recidiviz/tools/deploy/terraform_plan.sh recidiviz-staging dev-danawillow (check that the resource you imported is not being added or modified)

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../../script_base.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/../deploy_helpers.sh"

read -r -p "Project ID: " PROJECT_ID
read -r -p "TF state file prefix (e.g. dev-[yourname], leave empty during deploys): " TF_STATE_PREFIX

# Terraform determines certain resources by looking at the directory structure,
# so give our shell the ability to open plenty of file descriptors.
ulimit -n 1024 || exit_on_fail

reconfigure_terraform_backend "$PROJECT_ID" "$TF_STATE_PREFIX"

# At this point in the script, BASH_SOURCE_DIR is actually pointed to recidiviz/tools/deploy
# (not /oneoffs), because it gets overwritten when we source deploy_helpers.sh
DOCKER_TAG=$(terraform -chdir="${BASH_SOURCE_DIR}/terraform" output -raw docker_image_tag)
GIT_HASH=$(terraform -chdir="${BASH_SOURCE_DIR}/terraform" output -raw git_hash)

function terraform_import {
    RESOURCE_ADDR=$1
    RESOURCE_ID=$2

    terraform -chdir=./recidiviz/tools/deploy/terraform import \
    -var=project_id="$PROJECT_ID" \
    -var=docker_image_tag="$DOCKER_TAG" \
    -var=git_hash="$GIT_HASH" \
    "$RESOURCE_ADDR" "$RESOURCE_ID"
}

function terraform_rm {
    RESOURCE_ADDR=$1
    terraform -chdir=./recidiviz/tools/deploy/terraform state rm "$RESOURCE_ADDR"
}

function terraform_mv {
  OLD_RESOURCE_ADDR=$1
  UPDATED_RESOURCE_ADDR=$2
  terraform -chdir=./recidiviz/tools/deploy/terraform state mv \
    "$OLD_RESOURCE_ADDR" "$UPDATED_RESOURCE_ADDR"
}
