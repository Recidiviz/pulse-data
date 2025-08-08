#!/usr/bin/env bash

#
# Base script for deploying a new version to looker staging, whether it be in the context of cutting a release candidate for
# a cherry-pick or a standard release, or a standard alpha version deploy.
#
# This script is intended to be run from the pulse-data repo root, and it will clone the looker repo into a temporary
# directory, verify that the looker repo files are up-to-date with the pulse-data commit being deployed or will sync
# them if not by creating a new branch in the looker repo with the updated files. Once the looker repo files are up-to-date,
# it will create a new version tag in the looker repo, and point the looker project to the new tag.
#
BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/deploy_helpers.sh"
# shellcheck source=recidiviz/tools/deploy/looker_helpers.sh
source "${BASH_SOURCE_DIR}/looker_helpers.sh"

VERSION_TAG=''
COMMIT_HASH=''
BRANCH_NAME=''
DEBUG_BUILD_NAME=''
PROMOTE=''
NO_PROMOTE=''
LOOKER_PROJECT_ID='recidiviz-looker-staging'

function print_usage {
    echo_error "usage: $0 -v VERSION -c COMMIT_SHA [-p -n -d DEBUG_BUILD_NAME -r LOOKER_PROJECT_ID]"
    echo_error "  -v: Version tag to deploy (e.g. v1.2.0)"
    echo_error "  -c: Full SHA of the pulse-data commit that is being deployed."
    echo_error "  -b: Name of the branch from which we're deploying."
    echo_error "  -p: Indicates that we should point the looker project to the newly deployed version. Can not be used with -n."
    echo_error "  -n: Indicates that we should not point the looker project to the newly deployed version. Can not be used with -p."
    echo_error "  -d: Name to append to the version for local testing of the script (e.g. anna-test1)."
    echo_error "  -r: Project ID to deploy to. Defaults to recidiviz-staging."
    run_cmd exit 1
}

while getopts "b:v:c:pnd:r:" flag; do
  case "${flag}" in
    v) VERSION_TAG="$OPTARG" ;;
    c) RECIDIVIZ_DATA_COMMIT_HASH="$OPTARG" ;;
    b) BRANCH_NAME="$OPTARG" ;;
    p) PROMOTE='true';;
    n) NO_PROMOTE='true';;
    d) DEBUG_BUILD_NAME="$OPTARG" ;;
    r) LOOKER_PROJECT_ID="$OPTARG" ;;
    *) print_usage
       run_cmd exit 1 ;;
  esac
done

if [[ -z ${VERSION_TAG} ]]; then
    echo_error "Missing/empty version tag argument"
    print_usage
    run_cmd exit 1
fi

if [[ -z ${RECIDIVIZ_DATA_COMMIT_HASH} ]]; then
    echo_error "Missing/empty commit sha argument"
    print_usage
    run_cmd exit 1
fi

if [[ -z ${BRANCH_NAME} ]]; then
    echo_error "Missing/empty commit branch name argument"
    print_usage
    run_cmd exit 1
fi

if [[ (-n ${PROMOTE} && -n ${NO_PROMOTE}) ||  ( -z ${PROMOTE} && -z ${NO_PROMOTE}) ]]; then
    echo_error "Must pass exactly one of either -p (promote) or -n (no-promote) flags"
    print_usage
    run_cmd exit 1
fi

if [[ -n ${PROMOTE} && -n ${DEBUG_BUILD_NAME} ]]; then
    echo_error "Debug releases must only have  -n (no-promote) option."
    print_usage
    run_cmd exit 1
fi

clone_looker_repo_to_temp_dir

run_cmd safe_git_checkout_remote_branch "$BRANCH_NAME" "$TEMP_LOOKER_DIR" 

LOOKER_COMMIT_HASH=$(git -C "$TEMP_LOOKER_DIR" rev-parse HEAD) || exit_on_fail

if [[ -n ${DEBUG_BUILD_NAME} ]]; then
    VERSION_TAG=${VERSION_TAG}-${DEBUG_BUILD_NAME}
fi

if [[ -z ${DEBUG_BUILD_NAME} ]]; then
  LAST_DEPLOYED_GIT_VERSION_TAG=$(last_version_tag_on_branch "$BRANCH_NAME" "$TEMP_LOOKER_DIR") || exit_on_fail
  if ! version_less_than "$LAST_DEPLOYED_GIT_VERSION_TAG" "$VERSION_TAG"; then
      echo_error "Deploy version [$VERSION_TAG] must be greater than last deployed tag [$LAST_DEPLOYED_GIT_VERSION_TAG]."
      run_cmd exit 1
  fi
fi

if [[ -z ${DEBUG_BUILD_NAME} ]]; then
  echo "Creating local tag $VERSION_TAG"
  verify_hash "$LOOKER_COMMIT_HASH" "$TEMP_LOOKER_DIR"
  looker_git tag -m "Version $VERSION_TAG release - $(date +'%Y-%m-%d %H:%M:%S')" "$VERSION_TAG"

  echo "Pushing tags to remote"
  looker_git push origin --tags
fi

if [[ -n ${PROMOTE} ]]; then
  echo "Deploying Looker version $VERSION_TAG at commit ${LOOKER_COMMIT_HASH:0:7} to $LOOKER_PROJECT_ID."
  deploy_looker_staging_version "$VERSION_TAG" "$LOOKER_PROJECT_ID"
fi

echo "Deployed Looker version $VERSION_TAG to $LOOKER_PROJECT_ID."
