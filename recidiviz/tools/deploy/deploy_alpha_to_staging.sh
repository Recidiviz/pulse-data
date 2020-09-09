#!/usr/bin/env bash

#
# Script for deploying an alpha version to stage. A version deployed with this script will not be deployed to
# production.
#

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh
source ${BASH_SOURCE_DIR}/deploy_helpers.sh

echo "Verifying deploy permissions"
run_cmd verify_deploy_permissions

echo "Fetching all tags"
run_cmd git fetch --all --tags --prune

echo "Checking for existing tags at tip of master"
check_for_tags_at_branch_tip master

run_cmd safe_git_checkout_branch master

LAST_VERSION_TAG_ON_MASTER=$(last_version_tag_on_branch master) || exit_on_fail
NEW_VERSION=$(next_alpha_version ${LAST_VERSION_TAG_ON_MASTER}) || exit_on_fail

script_prompt "Will create tag and deploy version [$NEW_VERSION] at commit [$(git rev-parse HEAD)] which is the \
tip of branch [master]. Continue?"

echo "Creating local tag ${NEW_VERSION}"
run_cmd `git tag -m "Version $NEW_VERSION release - $(date +'%Y-%m-%d %H:%M:%S')" ${NEW_VERSION}`

echo "Pushing tags to remote"
run_cmd git push origin --tags

${BASH_SOURCE_DIR}/base_deploy_to_staging.sh -v ${NEW_VERSION} -p
