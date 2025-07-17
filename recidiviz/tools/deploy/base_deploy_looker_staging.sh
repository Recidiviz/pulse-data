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

RECIDIVIZ_DATA_REPO_ROOT=$(git rev-parse --show-toplevel) || exit_on_fail


# TODO(#44644) This should be moved to a post-merge CI job in the pulse-data repo.
function looker_generated_versions_match {
  verify_hash "$LOOKER_COMMIT_HASH" "$TEMP_LOOKER_DIR"
  LOOKER_HASH_FILE="${TEMP_LOOKER_DIR}/generated_version_hash"
  LOOKER_HASH=$(cat "$LOOKER_HASH_FILE") || exit_on_fail

  verify_hash "$RECIDIVIZ_DATA_COMMIT_HASH" "$RECIDIVIZ_DATA_REPO_ROOT"
  RECIDIVIZ_DATA_HASH_FILE="${RECIDIVIZ_DATA_REPO_ROOT}/recidiviz/tools/looker/generated_version_hash"
  RECIDIVIZ_DATA_HASH=$(cat "$RECIDIVIZ_DATA_HASH_FILE") || exit_on_fail

  [[ "$RECIDIVIZ_DATA_HASH" == "$LOOKER_HASH" ]]
}

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

if looker_generated_versions_match; then
  echo "Looker generated code is already up-to-date. Skipping sync."
else 
  echo "Looker generated code has changed. Proceeding to sync..."
  # TODO(#44644): This should be done in a separate script that is a part of the
  # pulse-data PR CI suite.

  verify_hash "$RECIDIVIZ_DATA_COMMIT_HASH" "$RECIDIVIZ_DATA_REPO_ROOT"
  run_cmd check_running_in_pipenv_shell

  run_cmd python -m recidiviz.tools.looker.copy_all_lookml --looker-repo-root "$TEMP_LOOKER_DIR"

  UPDATE_BRANCH_NAME="update-generated-files-$(date +%Y%m%d%H%M%S)"

  looker_git checkout -b "$UPDATE_BRANCH_NAME"
  looker_git add .
  looker_git commit -m "Sync generated Looker files from pulse-data@${VERSION_TAG}"
  looker_git push --set-upstream origin "$UPDATE_BRANCH_NAME"

  PR_URL="https://github.com/Recidiviz/looker/compare/${BRANCH_NAME}...${UPDATE_BRANCH_NAME}?expand=1"
  # TODO(#44643) Add lookml validation as part of looker repo CI.
  echo "Opening pull request: $PR_URL"

  open "$PR_URL"

  RECONSTRUCTED_CMD="./recidiviz/tools/deploy/base_deploy_looker_staging.sh"
  [ -n "$BRANCH_NAME" ] && RECONSTRUCTED_CMD+=" -b $BRANCH_NAME"
  [ -n "$VERSION_TAG" ] && RECONSTRUCTED_CMD+=" -v $VERSION_TAG"
  [ -n "$RECIDIVIZ_DATA_COMMIT_HASH" ] && RECONSTRUCTED_CMD+=" -c $RECIDIVIZ_DATA_COMMIT_HASH"
  [ "$PROMOTE" = "true" ] && RECONSTRUCTED_CMD+=" -p"
  [ "$NO_PROMOTE" = "true" ] && RECONSTRUCTED_CMD+=" -n"
  [ -n "$DEBUG_BUILD_NAME" ] && RECONSTRUCTED_CMD+=" -d $DEBUG_BUILD_NAME"
  [ -n "$LOOKER_PROJECT_ID" ] && RECONSTRUCTED_CMD+=" -r $LOOKER_PROJECT_ID"

  script_prompt "Has the pull request been merged? If the script was accidentally terminated, you can retry by running the following after the PR is merged:\n\t${RECONSTRUCTED_CMD}"

  run_cmd safe_git_checkout_remote_branch "$BRANCH_NAME" "$TEMP_LOOKER_DIR"
  LOOKER_COMMIT_HASH=$(git -C "$TEMP_LOOKER_DIR"  rev-parse HEAD) || exit_on_fail
  if ! looker_generated_versions_match; then
    echo_error "Looker generated code is still not up-to-date after syncing. Please check the PR and retry by running:\n\t${RECONSTRUCTED_CMD}"
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
