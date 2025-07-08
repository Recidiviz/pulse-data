#!/usr/bin/env bash
#
# Script for deploying tagged versions to looker production project.
#
BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/deploy_helpers.sh"
# shellcheck source=recidiviz/tools/deploy/looker_helpers.sh
source "${BASH_SOURCE_DIR}/looker_helpers.sh"

PROJECT_ID="recidiviz-looker-123"

if [[ "$1" == "" ]]; then
    echo_error "usage: $0 <version_tag>"
    exit 1
fi

GIT_VERSION_TAG=$(echo "$1" | tr '-' '.') || exit_on_fail
if [[ ! ${GIT_VERSION_TAG} =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo_error "Invalid release version [$GIT_VERSION_TAG] - must match regex: v[0-9]+\.[0-9]+\.[0-9]+"
    run_cmd exit 1
fi

clone_looker_repo_to_temp_dir

echo "Fetching all tags"
looker_git fetch --all --tags --prune --prune-tags --force

validate_release_branch_changes_since_tag "$GIT_VERSION_TAG" "$TEMP_LOOKER_DIR"

# Afaik there is currently no way to query the currently deployed version tag in a looker project.
# TODO(#36190) Think of a workaround for this.
# if ! version_less_than "${LAST_DEPLOYED_GIT_VERSION_TAG}" "${GIT_VERSION_TAG}"; then
#  echo_error "Deploy version [$GIT_VERSION_TAG] must be greater than last deployed tag [$LAST_DEPLOYED_GIT_VERSION_TAG]."
#  run_cmd exit 1
# fi

# echo "Beginning deploy of version [$GIT_VERSION_TAG] to production. Last deployed version: [$LAST_DEPLOYED_GIT_VERSION_TAG]."
# script_prompt "Do you want to continue?"

# echo "Commits since last deploy:"
# looker_git log --oneline "tags/${LAST_DEPLOYED_GIT_VERSION_TAG}..tags/${GIT_VERSION_TAG}"

deploy_looker_prod_version "$GIT_VERSION_TAG" "$LOOKER_PROJECT_ID"
