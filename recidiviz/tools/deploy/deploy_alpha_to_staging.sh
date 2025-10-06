#!/usr/bin/env bash

#
# Script for deploying an alpha version to stage. A version deployed with this script will not be deployed to
# production.
#

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/deploy_helpers.sh"

echo "Fetching all tags"
run_cmd git fetch --all --tags --prune --prune-tags --force

echo "Checking for existing tags at tip of main"
ALLOW_ALPHA_TAGS=false
MAIN_IS_TAGGED=$(check_for_tags_at_branch_tip main "${ALLOW_ALPHA_TAGS}") || exit_on_fail
if [[ $MAIN_IS_TAGGED -eq 1 ]]; then
        echo_error "The tip of branch [main] is already tagged for pulse-data repo - exiting."
        echo_error "If you believe this tag exists due a previously failed deploy attempt and you want to retry the "
        echo_error "deploy for this version, run \`git push --delete origin <tag name>\` to delete the old tag from"
        echo_error "remote, if it exists, and \`git tag -D <tag name>\` to delete it locally."
        exit 1
fi

run_cmd safe_git_checkout_remote_branch main

INCLUDE_ALPHA_VERSION_TAGS=true
LAST_VERSION_TAG_ON_MAIN=$(last_version_tag_on_branch main "${INCLUDE_ALPHA_VERSION_TAGS}") || exit_on_fail
NEW_VERSION=$(next_alpha_version "${LAST_VERSION_TAG_ON_MAIN}") || exit_on_fail

COMMIT_HASH=$(git rev-parse HEAD) || exit_on_fail
script_prompt "Will create tag and deploy version [$NEW_VERSION] at commit [${COMMIT_HASH:0:7}] which is the \
tip of branch [main]. Continue?"

"${BASH_SOURCE_DIR}/base_deploy_to_staging.sh" -v "${NEW_VERSION}" -c "${COMMIT_HASH}" -b main -p || exit_on_fail

NEW_ALPHA_DEPLOY_BRANCH="alpha/${NEW_VERSION}"

echo "Checking out new alpha deploy branch [$NEW_ALPHA_DEPLOY_BRANCH]"
run_cmd git checkout -b "${NEW_ALPHA_DEPLOY_BRANCH}"

echo "Pushing new alpha deploy branch [$NEW_ALPHA_DEPLOY_BRANCH] to remote"
run_cmd git push --set-upstream origin "${NEW_ALPHA_DEPLOY_BRANCH}"

echo "Returning to main"
run_cmd safe_git_checkout_remote_branch main

script_prompt "Have you completed all Post-Deploy tasks for this STAGING version in https://go/platform-deploy-log?"

echo "Alpha staging deploy complete."
