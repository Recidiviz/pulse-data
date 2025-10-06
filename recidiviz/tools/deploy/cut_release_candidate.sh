#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/deploy_helpers.sh"
# shellcheck source=recidiviz/tools/deploy/looker_helpers.sh
source "${BASH_SOURCE_DIR}/looker_helpers.sh"
PROJECT="recidiviz-staging"

# Used to track total time required to cut release candidate.
# See how this works at https://stackoverflow.com/questions/8903239/how-to-calculate-time-elapsed-in-bash-script.
SECONDS=0

FORCE_PROMOTE=''
function print_usage {
    echo_error "usage: $0 [-p] BRANCH"
    echo_error "  -p: Force the candidate to be promoted."
    run_cmd exit 1
}

while getopts "p" flag; do
  case "${flag}" in
    p) FORCE_PROMOTE='true';;
    *) print_usage
       run_cmd exit 1 ;;
  esac
done

if [[ "${*:$OPTIND:1}" == "" ]]; then
    print_usage
    run_cmd exit 1
fi

RELEASE_CANDIDATE_BASE_BRANCH=${*:$OPTIND:1}

if [[ ! ${RELEASE_CANDIDATE_BASE_BRANCH} == "main" && ! ${RELEASE_CANDIDATE_BASE_BRANCH} =~ ^releases\/v[0-9]+\.[0-9]+-rc$ ]]; then
    echo_error "Invalid base branch for release candidate - must be 'main' or a 'releases/*' branch"
    run_cmd exit 1
fi

# Ensure we have a local copy of the Looker repo before checking tags
clone_looker_repo_to_temp_dir

echo "Fetching all tags"
run_cmd git fetch --all --tags --prune --prune-tags --force
run_cmd git -C "${TEMP_LOOKER_DIR}" fetch --all --tags --prune --prune-tags --force

run_cmd safe_git_checkout_remote_branch "${RELEASE_CANDIDATE_BASE_BRANCH}"

echo "Checking for existing release tags at tip of [$RELEASE_CANDIDATE_BASE_BRANCH]"

ALLOW_ALPHA_TAGS=true
has_recidiviz_data_changes_to_release=$(check_for_tags_at_branch_tip "${RELEASE_CANDIDATE_BASE_BRANCH}" "${ALLOW_ALPHA_TAGS}")
has_looker_changes_to_release=$(check_for_tags_at_branch_tip "${RELEASE_CANDIDATE_BASE_BRANCH}" "${ALLOW_ALPHA_TAGS}" "${TEMP_LOOKER_DIR}")
if [[ -n "${has_recidiviz_data_changes_to_release}" && -n "${has_looker_changes_to_release}" ]]; then
        echo_error "The tip of branch [$RELEASE_CANDIDATE_BASE_BRANCH] is already tagged for both Recidiviz/pulse-data and Recidiviz/looker repos - exiting."
        echo_error "If you believe this tag exists due a previously failed deploy attempt and you want to retry the "
        echo_error "deploy for this version, run \`git push --delete origin <tag name>\` to delete the old tag from"
        echo_error "remote, if it exists, and \`git tag -D <tag name>\` to delete it locally."
        exit 1
fi

INCLUDE_ALPHA_VERSIONS_FLAG=true
LAST_VERSION_TAG_ON_BRANCH=$(last_version_tag_on_branch "${RELEASE_CANDIDATE_BASE_BRANCH}" "${INCLUDE_ALPHA_VERSIONS_FLAG}")

declare -a LAST_VERSION_PARTS
read -r -a LAST_VERSION_PARTS < <(parse_version "${LAST_VERSION_TAG_ON_BRANCH}")
MAJOR=${LAST_VERSION_PARTS[1]}
MINOR=${LAST_VERSION_PARTS[2]}
PATCH=${LAST_VERSION_PARTS[3]}
ALPHA=${LAST_VERSION_PARTS[4]-}  # Optional

if [[ "${RELEASE_CANDIDATE_BASE_BRANCH}" != "main" && -n "${ALPHA}" ]]; then
    echo_error "Found invalid previous tag on  a releases/* branch [$RELEASE_CANDIDATE_BASE_BRANCH]: $LAST_VERSION_TAG_ON_BRANCH"
    echo_error "Expected a version tag matching regex ^v[0-9]+.[0-9]+.[0-9]+$"
    run_cmd exit 1
fi

if [[ -n "${ALPHA}" ]]; then
    # If the previous version was an alpha version, merely strip the alpha term.
    RELEASE_VERSION_TAG="v$MAJOR.$MINOR.$PATCH"
    STAGING_PUSH_PROMOTE_FLAG='-p'
elif [[ ${RELEASE_CANDIDATE_BASE_BRANCH} == "main" ]]; then
    # If the previous version was a release version (i.e. alpha is absent) and we're creating a release candidate off
    # main, increment the minor version. The patch version will always be 0.
    RELEASE_VERSION_TAG="v$MAJOR.$((MINOR + 1)).0"
    STAGING_PUSH_PROMOTE_FLAG='-p'
else
    # If we're creating a cherry-pick release candidate on a releases branch, increment the patch version.
    RELEASE_VERSION_TAG="v$MAJOR.$MINOR.$((PATCH + 1))"
    # We do not want to promote traffic to this version since there may be newer versions already pushed to staging and
    # we don't want to regress functionality.
    STAGING_PUSH_PROMOTE_FLAG='-n'
fi

if [[ -n ${FORCE_PROMOTE} ]]; then
    STAGING_PUSH_PROMOTE_FLAG='-p'
fi

COMMIT_HASH=$(git rev-parse HEAD) || exit_on_fail
COMMIT_HASH_SHORT=${COMMIT_HASH:0:7}

echo "Generating release notes."
GITHUB_DEPLOY_BOT_TOKEN=$(get_secret "$PROJECT" github_deploy_script_pat) || exit_on_fail
run_cmd_no_exiting_no_echo pipenv run  python -m recidiviz.tools.deploy.check_for_prs \
  --base_branch "${RELEASE_CANDIDATE_BASE_BRANCH}" \
  --github_token "${GITHUB_DEPLOY_BOT_TOKEN}" || exit_on_fail

script_prompt "Will create tag and deploy version [$RELEASE_VERSION_TAG] at commit [${COMMIT_HASH_SHORT}] which is \
the tip of branch [$RELEASE_CANDIDATE_BASE_BRANCH]. Continue?"


"${BASH_SOURCE_DIR}/base_deploy_to_staging.sh" -v "${RELEASE_VERSION_TAG}" \
  -c "${COMMIT_HASH}" \
  -b "${RELEASE_CANDIDATE_BASE_BRANCH}" "${STAGING_PUSH_PROMOTE_FLAG}" || exit_on_fail

# Create and push a new releases branch
if [[ ${RELEASE_CANDIDATE_BASE_BRANCH} == "main" ]]; then
    declare -a NEW_VERSION_PARTS
    read -r -a NEW_VERSION_PARTS < <(parse_version "${RELEASE_VERSION_TAG}")

    NEW_MAJOR=${NEW_VERSION_PARTS[1]}
    NEW_MINOR=${NEW_VERSION_PARTS[2]}

    NEW_RELEASE_BRANCH="releases/v${NEW_MAJOR}.${NEW_MINOR}-rc"

    # We want this branch to be created before the pulse-data branch so the `Verify LookML autogenerated file
    # contents match between repos` check in pulse-data can pass.
    create_looker_release_branch "${NEW_RELEASE_BRANCH}" "${RELEASE_VERSION_TAG}"

    echo "Checking out new release branch [$NEW_RELEASE_BRANCH]"
    run_cmd git checkout -b "${NEW_RELEASE_BRANCH}"

    echo "Pushing new release branch [$NEW_RELEASE_BRANCH] to remote"
    run_cmd git push --set-upstream origin "${NEW_RELEASE_BRANCH}"

    script_prompt "Have you completed all Post-Deploy tasks for this release candidate staging version in https://go/platform-deploy-log?"
fi

duration=$SECONDS
MINUTES=$((duration / 60))
echo "Release candidate staging deploy completed in ${MINUTES} minutes."
