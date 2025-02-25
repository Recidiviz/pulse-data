#!/usr/bin/env bash

: "Script to determine the versions with which to tag the next backend and frontend
commits that are being deployed. We look up previous tags and increment the minor
version by 1.

Example usage:
./recidiviz/tools/deploy/justice_counts/get_next_version.sh
"

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../../script_base.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/../deploy_helpers.sh"

VERSION=''

function get_next_version {
    run_cmd git fetch --quiet --all --tags --prune --prune-tags --force

    LAST_VERSION_TAG_ON_BRANCH=$(git tag | grep "jc" | sort_versions | tail -n 1) || exit_on_fail

    if [[ ! ${LAST_VERSION_TAG_ON_BRANCH} =~ ^jc.v([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        echo_error "Invalid version - must be of the format vX.Y.Z"
        run_cmd exit 1
    fi

    MAJOR=${BASH_REMATCH[1]}
    MINOR=${BASH_REMATCH[2]}

    VERSION="v${MAJOR}.$((MINOR+1)).0"
}

run_cmd check_running_in_pipenv_shell

get_next_version || exit_on_fail

# Returns the next tag version.
echo "${VERSION}"
