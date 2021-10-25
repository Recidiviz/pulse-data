#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh
source ${BASH_SOURCE_DIR}/deploy_helpers.sh

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

if [[ x"${@:$OPTIND:1}" == x ]]; then
    print_usage
    run_cmd exit 1
fi

RELEASE_CANDIDATE_BASE_BRANCH=${@:$OPTIND:1}

if [[ ! ${RELEASE_CANDIDATE_BASE_BRANCH} == "main" && ! ${RELEASE_CANDIDATE_BASE_BRANCH} =~ ^releases\/v[0-9]+\.[0-9]+-rc$ ]]; then
    echo_error "Invalid base branch for release candidate - must be 'main' or a 'releases/*' branch"
    run_cmd exit 1
fi

echo "Fetching all tags"
run_cmd git fetch --all --tags --prune --prune-tags

run_cmd safe_git_checkout_remote_branch ${RELEASE_CANDIDATE_BASE_BRANCH}

echo "Checking for existing release tags at tip of [$RELEASE_CANDIDATE_BASE_BRANCH]"
check_for_tags_at_branch_tip ${RELEASE_CANDIDATE_BASE_BRANCH} ALLOW_ALPHA

LAST_VERSION_TAG_ON_BRANCH=$(last_version_tag_on_branch ${RELEASE_CANDIDATE_BASE_BRANCH})

LAST_VERSION_PARTS=($(parse_version ${LAST_VERSION_TAG_ON_BRANCH}))
MAJOR=${LAST_VERSION_PARTS[1]}
MINOR=${LAST_VERSION_PARTS[2]}
PATCH=${LAST_VERSION_PARTS[3]}
ALPHA=${LAST_VERSION_PARTS[4]-}  # Optional

if [[ ${RELEASE_CANDIDATE_BASE_BRANCH} != "main" && ! -z ${ALPHA} ]]; then
    echo_error "Found invalid previous tag on  a releases/* branch [$RELEASE_CANDIDATE_BASE_BRANCH]: $LAST_VERSION_TAG_ON_BRANCH"
    echo_error "Expected a version tag matching regex ^v[0-9]+.[0-9]+.[0-9]+$"
    run_cmd exit 1
fi

if [[ ! -z ${ALPHA} ]]; then
    # If the previous version was an alpha version, merely strip the alpha term.
    RELEASE_VERSION_TAG="v$MAJOR.$MINOR.$PATCH"
    STAGING_PUSH_PROMOTE_FLAG='-p'
elif [[ ${RELEASE_CANDIDATE_BASE_BRANCH} == "main" ]]; then
    # If the previous version was a release version (i.e. alpha is absent) and we're creating a release candidate off
    # main, increment the minor version. The patch version will always be 0.
    RELEASE_VERSION_TAG="v$MAJOR.$(($MINOR + 1)).0"
    STAGING_PUSH_PROMOTE_FLAG='-p'
else
    # If we're creating a cherry-pick release candidate on a releases branch, increment the patch version.
    RELEASE_VERSION_TAG="v$MAJOR.$MINOR.$(($PATCH + 1))"
    # We do not want to promote traffic to this version since there may be newer versions already pushed to staging and
    # we don't want to regress functionality.
    STAGING_PUSH_PROMOTE_FLAG='-n'
fi

if [[ ! -z ${FORCE_PROMOTE} ]]; then
    STAGING_PUSH_PROMOTE_FLAG='-p'
fi

COMMIT_HASH=$(git rev-parse HEAD) || exit_on_fail
script_prompt "Will create tag and deploy version [$RELEASE_VERSION_TAG] at commit [${COMMIT_HASH:0:7}] which is \
the tip of branch [$RELEASE_CANDIDATE_BASE_BRANCH]. Continue?"

${BASH_SOURCE_DIR}/base_deploy_to_staging.sh -v ${RELEASE_VERSION_TAG} -c ${COMMIT_HASH} -b ${RELEASE_CANDIDATE_BASE_BRANCH} ${STAGING_PUSH_PROMOTE_FLAG} || exit_on_fail

echo "Deploy succeeded - creating local tag [${RELEASE_VERSION_TAG}]"
verify_hash $COMMIT_HASH
run_cmd `git tag -m "Version [$RELEASE_VERSION_TAG] release - $(date +'%Y-%m-%d %H:%M:%S')" ${RELEASE_VERSION_TAG}`

echo "Pushing tags to remote"
run_cmd git push origin --tags

# Create and push a new releases branch
if [[ ${RELEASE_CANDIDATE_BASE_BRANCH} == "main" ]]; then
    NEW_VERSION_PARTS=($(parse_version ${RELEASE_VERSION_TAG}))

    NEW_MAJOR=${NEW_VERSION_PARTS[1]}
    NEW_MINOR=${NEW_VERSION_PARTS[2]}

    NEW_RELEASE_BRANCH="releases/v${NEW_MAJOR}.${NEW_MINOR}-rc"

    echo "Checking out new release branch [$NEW_RELEASE_BRANCH]"
    run_cmd git checkout -b ${NEW_RELEASE_BRANCH}

    echo "Pushing new release branch [$NEW_RELEASE_BRANCH] to remote"
    run_cmd git push --set-upstream origin ${NEW_RELEASE_BRANCH}
fi

echo "Release candidate staging deploy complete."
