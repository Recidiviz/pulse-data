#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh
source ${BASH_SOURCE_DIR}/deploy_helpers.sh

DEBUG_BUILD_NAME=''

function print_usage {
    echo_error "usage: $0 -d DEBUG_BUILD_NAME"
    echo_error "  -d: Name to append to the version for a debug local deploy (e.g. anna-test1)."
    exit 1
}

while getopts "d:" flag; do
  case "${flag}" in
    d) DEBUG_BUILD_NAME="$OPTARG" ;;
    *) print_usage
       exit 1 ;;
  esac
done

if [[ -z ${DEBUG_BUILD_NAME} ]]; then
    print_usage
    exit 1
fi

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

echo "Fetching all tags"
run_cmd git fetch --all --tags --prune

LAST_VERSION_TAG_ON_CURRENT_BRANCH=$(last_version_tag_on_branch HEAD)
LAST_VERSION_TAG_ON_MASTER=$(last_version_tag_on_branch master)

if [[ ${LAST_VERSION_TAG_ON_CURRENT_BRANCH} != ${LAST_VERSION_TAG_ON_MASTER} ]]; then
    echo_error "Current branch does not contain latest version tag on master [$LAST_VERSION_TAG_ON_MASTER] - please rebase."
    exit 1
fi

VERSION_TAG=$(next_alpha_version ${LAST_VERSION_TAG_ON_MASTER}) || exit_on_fail

# Deploys a debug version to staging without promoting traffic to it
${BASH_SOURCE_DIR}/base_deploy_to_staging.sh -v ${VERSION_TAG} -d ${DEBUG_BUILD_NAME} -n
