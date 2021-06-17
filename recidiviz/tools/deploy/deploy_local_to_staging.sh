#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh
source ${BASH_SOURCE_DIR}/deploy_helpers.sh

DEBUG_BUILD_NAME=''
PROJECT_ID='recidiviz-staging'

function print_usage {
    echo_error "usage: $0 -d DEBUG_BUILD_NAME [-r PROJECT_ID]"
    echo_error "  -d: Name to append to the version for a debug local deploy (e.g. anna-test1)."
    echo_error "  -r: Project ID to deploy to. Defaults to recidiviz-staging."
    run_cmd exit 1
}

while getopts "d:r:" flag; do
  case "${flag}" in
    d) DEBUG_BUILD_NAME="$OPTARG" ;;
    r) PROJECT_ID="$OPTARG" ;;
    *) print_usage
       run_cmd exit 1 ;;
  esac
done

if [[ -z ${DEBUG_BUILD_NAME} ]]; then
    print_usage
    run_cmd exit 1
fi

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

echo "Fetching all tags"
run_cmd git fetch --all --tags --prune --prune-tags

LAST_VERSION_TAG_ON_CURRENT_BRANCH=$(last_version_tag_on_branch HEAD)
LAST_VERSION_TAG_ON_MASTER=$(last_version_tag_on_branch master)

if [[ ${LAST_VERSION_TAG_ON_CURRENT_BRANCH} != ${LAST_VERSION_TAG_ON_MASTER} ]]; then
    echo_error "Current branch does not contain latest version tag on master [$LAST_VERSION_TAG_ON_MASTER] - please rebase."
    run_cmd exit 1
fi

VERSION_TAG=$(next_alpha_version ${LAST_VERSION_TAG_ON_MASTER}) || exit_on_fail

# Deploys a debug version to staging without promoting traffic to it
COMMIT_HASH=$(git rev-parse HEAD) || exit_on_fail
${BASH_SOURCE_DIR}/base_deploy_to_staging.sh -v ${VERSION_TAG} -c ${COMMIT_HASH} -d ${DEBUG_BUILD_NAME} -n  -r ${PROJECT_ID}|| exit_on_fail

echo "Local to staging deploy complete."
