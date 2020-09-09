#!/usr/bin/env bash

if [[ x"$1" == x ]]; then
    echo_error "usage: $0 <base_branch>"
    exit 1
fi

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh
source ${BASH_SOURCE_DIR}/deploy_helpers.sh

RELEASE_CANDIDATE_BASE_BRANCH=$1

if [[ ! ${RELEASE_CANDIDATE_BASE_BRANCH} == "master" && ! ${RELEASE_CANDIDATE_BASE_BRANCH} =~ ^releases\/v[0-9]+\.[0-9]+-rc$ ]]; then
    echo_error "Invalid base branch for release candidate - must be 'master' or a 'releases/*' branch"
    exit 1
fi

echo "Verifying deploy permissions"
run_cmd verify_deploy_permissions

echo "Fetching all tags"
run_cmd git fetch --all --tags --prune

run_cmd safe_git_checkout_branch ${RELEASE_CANDIDATE_BASE_BRANCH}

HEAD_COMMIT=$(git rev-parse HEAD) || exit_on_fail

echo "Checking for green build at HEAD commit [$HEAD_COMMIT]"
run_cmd check_commit_is_green ${HEAD_COMMIT}

echo "Checking for existing release tags at tip of [$RELEASE_CANDIDATE_BASE_BRANCH]"
check_for_tags_at_branch_tip ${RELEASE_CANDIDATE_BASE_BRANCH} ALLOW_ALPHA

LAST_VERSION_TAG_ON_BRANCH=$(last_version_tag_on_branch ${RELEASE_CANDIDATE_BASE_BRANCH})

LAST_VERSION_PARTS=($(parse_version ${LAST_VERSION_TAG_ON_BRANCH}))
MAJOR=${LAST_VERSION_PARTS[1]}
MINOR=${LAST_VERSION_PARTS[2]}
PATCH=${LAST_VERSION_PARTS[3]}
ALPHA=${LAST_VERSION_PARTS[4]-}  # Optional

if [[ ${RELEASE_CANDIDATE_BASE_BRANCH} != "master" && ! -z ${ALPHA} ]]; then
    echo_error "Found invalid previous tag on  a releases/* branch [$RELEASE_CANDIDATE_BASE_BRANCH]: $LAST_VERSION_TAG_ON_BRANCH"
    echo_error "Expected a version tag matching regex ^v[0-9]+.[0-9]+.[0-9]+$"
    exit 1
fi

if [[ ! -z ${ALPHA} ]]; then
    # If the previous version was an alpha version, merely strip the alpha term.
    RELEASE_VERSION_TAG="v$MAJOR.$MINOR.$PATCH"
    STAGING_PUSH_PROMOTE_FLAG='-p'
elif [[ ${RELEASE_CANDIDATE_BASE_BRANCH} == "master" ]]; then
    # If the previous version was a release version (i.e. alpha is absent) and we're creating a release candidate off
    # master, increment the minor version. The patch version will always be 0.
    RELEASE_VERSION_TAG="v$MAJOR.$(($MINOR + 1)).0"
    STAGING_PUSH_PROMOTE_FLAG='-p'
else
    # If we're creating a cherry-pick release candidate on a releases branch, increment the patch version.
    RELEASE_VERSION_TAG="v$MAJOR.$MINOR.$(($PATCH + 1))"
    # We do not want to promote traffic to this version since there may be newer versions already pushed to staging and
    # we don't want to regress functionality.
    STAGING_PUSH_PROMOTE_FLAG='-n'
fi

script_prompt "Will create tag and deploy version [$RELEASE_VERSION_TAG] at commit [$(git rev-parse HEAD)] which is \
the tip of branch [$RELEASE_CANDIDATE_BASE_BRANCH]. Continue?"

echo "Creating local tag [${RELEASE_VERSION_TAG}]"
run_cmd `git tag -m "Version [$RELEASE_VERSION_TAG] release - $(date +'%Y-%m-%d %H:%M:%S')" ${RELEASE_VERSION_TAG}`

echo "Pushing tags to remote"
run_cmd git push origin --tags

${BASH_SOURCE_DIR}/base_deploy_to_staging.sh -v ${RELEASE_VERSION_TAG} ${STAGING_PUSH_PROMOTE_FLAG}

# Create and push a new releases branch
if [[ ${RELEASE_CANDIDATE_BASE_BRANCH} == "master" ]]; then
    NEW_VERSION_PARTS=($(parse_version ${RELEASE_VERSION_TAG}))

    NEW_MAJOR=${NEW_VERSION_PARTS[1]}
    NEW_MINOR=${NEW_VERSION_PARTS[2]}

    NEW_RELEASE_BRANCH="releases/${NEW_MAJOR}.${NEW_MINOR}-rc"

    echo "Checking out new release branch [$NEW_RELEASE_BRANCH]"
    run_cmd git checkout -b ${NEW_RELEASE_BRANCH}

    echo "Pushing new release branch [$NEW_RELEASE_BRANCH] to remote"
    run_cmd git push --set-upstream origin ${NEW_RELEASE_BRANCH}
fi
