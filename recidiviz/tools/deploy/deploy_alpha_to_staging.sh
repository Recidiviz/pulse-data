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
check_for_tags_at_branch_tip main

run_cmd safe_git_checkout_remote_branch main

LAST_VERSION_TAG_ON_MAIN=$(last_version_tag_on_branch main) || exit_on_fail
NEW_VERSION=$(next_alpha_version "${LAST_VERSION_TAG_ON_MAIN}") || exit_on_fail

COMMIT_HASH=$(git rev-parse HEAD) || exit_on_fail
script_prompt "Will create tag and deploy version [$NEW_VERSION] at commit [${COMMIT_HASH:0:7}] which is the \
tip of branch [main]. Continue?"

"${BASH_SOURCE_DIR}/base_deploy_to_staging.sh" -v "${NEW_VERSION}" -c "${COMMIT_HASH}" -b main -p || exit_on_fail
"${BASH_SOURCE_DIR}/base_deploy_looker_staging.sh" -v "${NEW_VERSION}" -c "${COMMIT_HASH}" -b main -p || exit_on_fail

NEW_ALPHA_DEPLOY_BRANCH="alpha/${NEW_VERSION}"

echo "Checking out new alpha deploy branch [$NEW_ALPHA_DEPLOY_BRANCH]"
run_cmd git checkout -b "${NEW_ALPHA_DEPLOY_BRANCH}"

echo "Pushing new alpha deploy branch [$NEW_ALPHA_DEPLOY_BRANCH] to remote"
run_cmd git push --set-upstream origin "${NEW_ALPHA_DEPLOY_BRANCH}"

echo "Returning to main"
run_cmd safe_git_checkout_remote_branch main

echo "Alpha staging deploy complete."
