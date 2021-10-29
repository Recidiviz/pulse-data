#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh
source ${BASH_SOURCE_DIR}/deploy_helpers.sh

# Check that a project_id parameter was passed in
# -z checks that the length of the string is 0
# "${1:+x} is parameter expansion, if param is null or unset it returns nothing
if [[ -z "${1:+x}" ]]; then
    echo_error "usage: $0 <project_id>"
    run_cmd exit 1
fi

PROJECT_ID=$1

echo "##### Running for project $PROJECT_ID ########"

run_cmd git fetch --all --tags --prune --prune-tags

DOCKER_IMAGE_TAG=$(last_version_tag_on_branch HEAD) || exit_on_fail
LAST_DEPLOYED_VERSION_TAG=$(last_deployed_version_tag $PROJECT_ID) || exit_on_fail

if [[ ! ${LAST_DEPLOYED_VERSION_TAG} == ${DOCKER_IMAGE_TAG} ]]; then
    echo_error "Most recent version tag on this branch [${DOCKER_IMAGE_TAG}] does not "
    echo_error "match last deployed version tag [${LAST_DEPLOYED_VERSION_TAG}] in "
    echo_error "project [$PROJECT_ID]. Make sure you have rebased on the latest "
    echo_error "version of the released branch."
    run_cmd exit 1
fi

GIT_HASH=$(git rev-list -n 1 tags/$DOCKER_IMAGE_TAG) || exit_on_fail
STATE_BUCKET="${PROJECT_ID}-tf-state"


TERRAFORM_ROOT_PATH='recidiviz/tools/deploy/terraform'
LOG_PATH="${TERRAFORM_ROOT_PATH}/.terraform/debug_`date +%Y-%m-%dT%H:%M:%S`.txt"

function terraform_with_debug {
  echo "##### Debug logs can be found at [${LOG_PATH}] ########"
  TF_LOG=DEBUG TF_LOG_PATH="$LOG_PATH" `which terraform` "$@"
}

echo "##### Initializing Terraform ########"
terraform_with_debug -chdir=$TERRAFORM_ROOT_PATH init \
  -backend-config "bucket=${STATE_BUCKET}" \
  -reconfigure || exit_on_fail

echo "##### Planning Terraform ########"
terraform_with_debug -chdir=$TERRAFORM_ROOT_PATH plan \
  -var="project_id=${PROJECT_ID}" \
  -var="git_hash=${GIT_HASH}" \
  -var="docker_image_tag=${DOCKER_IMAGE_TAG}" || exit_on_fail

echo "##### Done with plan ########"
