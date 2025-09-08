#!/usr/bin/env bash
# 
# Script to run `terraform plan`, setting necessary variables to their correct values.
# 
# Takes two positional arguments: project id (required) and terraform state file prefix (optional).
# The state file prefix is used to run against a state file in a subdirectory of the bucket, and
# will typically be called dev-[yourname].
# 
# Example usage:
#   ./recidiviz/tools/deploy/terraform_plan.sh recidiviz-staging
#   ./recidiviz/tools/deploy/terraform_plan.sh recidiviz-staging dev-danawillow

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/deploy_helpers.sh"

# Check that a project_id parameter was passed in
# -z checks that the length of the string is 0
# "${1:+x} is parameter expansion, if param is null or unset it returns nothing
if [[ -z "${1:+x}" ]]; then
    echo_error "usage: $0 <project_id> [tf_state_prefix]"
    run_cmd exit 1
fi

PROJECT_ID=$1
TF_STATE_PREFIX=${2:-""}

echo "##### Running for project $PROJECT_ID ########"

run_cmd git fetch --all --tags --prune --prune-tags --force

INCLUDE_ALPHA_VERSION_TAGS=true
DOCKER_IMAGE_TAG=$(last_version_tag_on_branch HEAD "${INCLUDE_ALPHA_VERSION_TAGS}") || exit_on_fail
LAST_DEPLOYED_VERSION_TAG=$(last_deployed_version_tag "${PROJECT_ID}") || exit_on_fail

if [[ ! ${LAST_DEPLOYED_VERSION_TAG} == "${DOCKER_IMAGE_TAG}" ]]; then
    echo_error "Most recent version tag on this branch [${DOCKER_IMAGE_TAG}] does not "
    echo_error "match last deployed version tag [${LAST_DEPLOYED_VERSION_TAG}] in "
    echo_error "project [$PROJECT_ID]. Make sure you have rebased on the latest "
    echo_error "version of the released branch."
    run_cmd exit 1
fi

GIT_HASH=$(git rev-list -n 1 tags/"${DOCKER_IMAGE_TAG}") || exit_on_fail
PAGERDUTY_TOKEN=$(get_secret "$PROJECT_ID" pagerduty_terraform_key) || exit_on_fail


TERRAFORM_ROOT_PATH='recidiviz/tools/deploy/terraform'
LOG_PATH="${TERRAFORM_ROOT_PATH}/.terraform/debug_$(date +%Y-%m-%dT%H:%M:%S).txt"

function terraform_with_debug {
  echo "##### Debug logs can be found at [${LOG_PATH}] ########"
  TF_LOG=DEBUG TF_LOG_PATH="$LOG_PATH" $(which terraform) "$@"
}

echo "##### Initializing Terraform ########"
reconfigure_terraform_backend "${PROJECT_ID}" "${TF_STATE_PREFIX}" || exit_on_fail


echo "##### Planning Terraform ########"
terraform_with_debug -chdir=$TERRAFORM_ROOT_PATH plan \
  -var="project_id=${PROJECT_ID}" \
  -var="git_hash=${GIT_HASH}" \
  -var="pagerduty_token=${PAGERDUTY_TOKEN}" \
  -var="docker_image_tag=${DOCKER_IMAGE_TAG}" \
  -parallelism=64 || exit_on_fail

echo "##### Done with plan ########"
