#!/usr/bin/env bash

#
# Base script for deploying a new version to staging, whether it be a cherry-pick, a standard release, or a new alpha
# version.
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
PROJECT_ID='recidiviz-staging'
LOOKER_PROJECT_ID='recidiviz-looker-staging'

function print_usage {
    echo_error "usage: $0 -v VERSION -c COMMIT_SHA [-p -n -d DEBUG_BUILD_NAME -r PROJECT_ID]"
    echo_error "  -v: Version tag to deploy (e.g. v1.2.0)"
    echo_error "  -c: Full SHA of the commit that is to be deployed."
    echo_error "  -b: Name of the branch from which we're deploying. (required if promoting)"
    echo_error "  -p: Indicates that we should promote traffic to the newly deployed version. Can not be used with -n."
    echo_error "  -n: Indicates that we should not promote traffic to the newly deployed version. Can not be used with -p."
    echo_error "  -d: Name to append to the version for a debug local deploy (e.g. anna-test1)."
    echo_error "  -r: Project ID to deploy to. Defaults to recidiviz-staging."
    run_cmd exit 1
}

while getopts "b:v:c:pnd:r:" flag; do
  case "${flag}" in
    v) VERSION_TAG="$OPTARG" ;;
    c) COMMIT_HASH="$OPTARG" ;;
    b) BRANCH_NAME="$OPTARG" ;;
    p) PROMOTE='true';;
    n) NO_PROMOTE='true';;
    d) DEBUG_BUILD_NAME="$OPTARG" ;;
    r) PROJECT_ID="$OPTARG" ;;
    *) print_usage
       run_cmd exit 1 ;;
  esac
done

if [[ -z ${VERSION_TAG} ]]; then
    echo_error "Missing/empty version tag argument"
    print_usage
    run_cmd exit 1
fi

if [[ -z ${COMMIT_HASH} ]]; then
    echo_error "Missing/empty commit sha argument"
    print_usage
    run_cmd exit 1
fi

if [[ -z ${BRANCH_NAME} && -n ${PROMOTE} ]]; then
    echo_error "Missing/empty commit branch name while promoting"
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

echo "Performing pre-deploy verification"
verify_hash "$COMMIT_HASH"
run_cmd verify_can_deploy "$PROJECT_ID" "${COMMIT_HASH}"

if [[ -n ${DEBUG_BUILD_NAME} ]]; then
    VERSION_TAG=${VERSION_TAG}-${DEBUG_BUILD_NAME}
fi
DOCKER_IMAGE_TAG=${VERSION_TAG}

if [[ -n ${PROMOTE} ]]; then
  script_prompt "Have you completed all Pre-Deploy tasks for this STAGING version in https://go/platform-deploy-log?"
fi

update_deployment_status "${DEPLOYMENT_STATUS_STARTED}" "${PROJECT_ID}" "${COMMIT_HASH:0:7}" "${VERSION_TAG}"


if [[ -n ${PROMOTE} ]]; then
    INCLUDE_ALPHA_VERSIONS_FLAG=true
else
    # We run a no-promote deploy when cutting a cherry-pick release candidate off a releases/* branch.
    # If a repo isn't committed to that often (like the Recidiviz/looker repo) we may run into the case where:
    # 1. a release candidate is cut on Friday which tags commit abcdefg123 with tag v1.123.0
    # 2. there are no new commits on `main` and the Monday staging deploy tags commit abcdefg123 with tag v1.124.0-alpha.0
    # 3. a cherry-pick release candidate is cut, attempting to create version v1.123.1
    # 4. v1.123.1 is less than v1.124.0-alpha.0 so the deploy would fail
    # To avoid this, we exclude alpha versions in the search when determining the last deployed version tag on the branch.
    INCLUDE_ALPHA_VERSIONS_FLAG=false
fi

LAST_DEPLOYED_GIT_VERSION_TAG=$(last_version_tag_on_branch "${BRANCH_NAME}" "${INCLUDE_ALPHA_VERSIONS_FLAG}") || exit_on_fail
if ! version_less_than "${LAST_DEPLOYED_GIT_VERSION_TAG}" "${VERSION_TAG}"; then
    echo_error "Recidiviz/pulse-data deploy version [$VERSION_TAG] must be greater than last deployed tag [$LAST_DEPLOYED_GIT_VERSION_TAG]."
    run_cmd exit 1
fi

clone_looker_repo_to_temp_dir
looker_git fetch --all --tags --prune --prune-tags --force
run_cmd safe_git_checkout_remote_branch "$BRANCH_NAME" "$TEMP_LOOKER_DIR"
LOOKER_COMMIT_HASH=$(git -C "$TEMP_LOOKER_DIR" rev-parse HEAD) || exit_on_fail
LAST_DEPLOYED_GIT_VERSION_TAG=$(last_version_tag_on_branch "$BRANCH_NAME" "$INCLUDE_ALPHA_VERSIONS_FLAG" "$TEMP_LOOKER_DIR") || exit_on_fail
if ! version_less_than "$LAST_DEPLOYED_GIT_VERSION_TAG" "$VERSION_TAG"; then
    echo_error "Recidiviz/looker deploy version [$VERSION_TAG] must be greater than last deployed tag [$LAST_DEPLOYED_GIT_VERSION_TAG]."
    run_cmd exit 1
fi


# app engine deploy paths
APP_ENGINE_IMAGE_BASE=us-docker.pkg.dev/$PROJECT_ID/appengine/default
APP_ENGINE_IMAGE_URL=$APP_ENGINE_IMAGE_BASE:${DOCKER_IMAGE_TAG} || exit_on_fail

APP_ENGINE_REMOTE_BUILD_BASE=us-docker.pkg.dev/$PROJECT_ID/appengine/build
APP_ENGINE_REMOTE_BUILD_URL=$APP_ENGINE_REMOTE_BUILD_BASE:${COMMIT_HASH}

# dataflow deploy paths
DATAFLOW_IMAGE_BASE=us-docker.pkg.dev/$PROJECT_ID/dataflow/default
DATAFLOW_IMAGE_URL=$DATAFLOW_IMAGE_BASE:${DOCKER_IMAGE_TAG} || exit_on_fail

DATAFLOW_BUILD_BASE=us-docker.pkg.dev/$PROJECT_ID/dataflow/build
DATAFLOW_BUILD_URL=$DATAFLOW_BUILD_BASE:${COMMIT_HASH}

# asset generation service deploy paths
ASSET_GENERATION_IMAGE_BASE=us-docker.pkg.dev/$PROJECT_ID/asset-generation/default
ASSET_GENERATION_IMAGE_URL=$ASSET_GENERATION_IMAGE_BASE:${DOCKER_IMAGE_TAG} || exit_on_fail

ASSET_GENERATION_BUILD_BASE=us-docker.pkg.dev/$PROJECT_ID/asset-generation/build
ASSET_GENERATION_BUILD_URL=$ASSET_GENERATION_BUILD_BASE:${COMMIT_HASH}

if [[ -n ${DEBUG_BUILD_NAME} ]]; then
    # Local debug build, we'll use docker on our local machine
    echo "Building docker image"
    verify_hash "$COMMIT_HASH"
    export DOCKER_BUILDKIT=1
    run_cmd docker build --pull \
        -t recidiviz-image                           \
        --target=recidiviz-app                       \
        --platform=linux/amd64                       \
        --build-arg=CURRENT_GIT_SHA="${COMMIT_HASH}" \
        .

    echo "Tagging app engine image url [$APP_ENGINE_IMAGE_URL] as recidiviz-image"
    verify_hash "$COMMIT_HASH"
    run_cmd docker tag recidiviz-image "${APP_ENGINE_IMAGE_URL}"

    echo "Pushing app engine image url [$APP_ENGINE_IMAGE_URL]"
    verify_hash "$COMMIT_HASH"
    run_cmd docker push "${APP_ENGINE_IMAGE_URL}"
else
    echo "Looking for remote App Engine, Dataflow, and Asset Generation service builds for commit ${COMMIT_HASH} on branch ${BRANCH_NAME}"
    verify_hash "$COMMIT_HASH"
    FOUND_APP_ENGINE_BUILD=false
    FOUND_DATAFLOW_BUILD=false
    FOUND_ASSET_GENERATION_BUILD=false
    ((timeout=300)) # 5 minute timeout

    while { [[ "${FOUND_APP_ENGINE_BUILD}" == "false" ]] || [[ "${FOUND_DATAFLOW_BUILD}" == "false" ]] || [[ "${FOUND_ASSET_GENERATION_BUILD}" == "false" ]]; } && ((timeout > 0));
    do
        ae_existing_tags=$(gcloud container images list-tags --filter="tags:${COMMIT_HASH}" --format=json "${APP_ENGINE_REMOTE_BUILD_BASE}")
        if [[ "$ae_existing_tags" != "[]" ]]; then
            FOUND_APP_ENGINE_BUILD=true
        else
            FOUND_APP_ENGINE_BUILD=false
        fi

        df_existing_tags=$(gcloud container images list-tags --filter="tags:${COMMIT_HASH}" --format=json "${DATAFLOW_BUILD_BASE}")
        if [[ "$df_existing_tags" != "[]" ]]; then
            FOUND_DATAFLOW_BUILD=true
        else
            FOUND_DATAFLOW_BUILD=false
        fi

        ag_existing_tags=$(gcloud container images list-tags --filter="tags:${COMMIT_HASH}" --format=json "${ASSET_GENERATION_BUILD_BASE}")
        if [[ "$ag_existing_tags" != "[]" ]]; then
            FOUND_ASSET_GENERATION_BUILD=true
        else
            FOUND_ASSET_GENERATION_BUILD=false
        fi

        if [[ "${FOUND_APP_ENGINE_BUILD}" == "true" ]] && [[ "${FOUND_DATAFLOW_BUILD}" == "true" ]] && [[ "${FOUND_ASSET_GENERATION_BUILD}" == "true" ]]; then
            break
        fi

        echo "Remote App Engine, Dataflow, and/or Asset Generation service builds for commit ${COMMIT_HASH} not found, retrying in 30s"
        sleep 30
        ((timeout -= 30))
    done

    if [[ "${FOUND_APP_ENGINE_BUILD}" == "false" ]]; then
      echo "Unable to find remote App Engine build for ${COMMIT_HASH} within the timeout - you might need to manually trigger it in Cloud Build (https://console.cloud.google.com/cloud-build/triggers?project=$PROJECT_ID). Exiting..."
    fi
    if [[ "${FOUND_DATAFLOW_BUILD}" == "false" ]]; then
      echo "Unable to find remote Dataflow build for ${COMMIT_HASH} within the timeout - you might need to manually trigger it in Cloud Build (https://console.cloud.google.com/cloud-build/triggers?project=$PROJECT_ID). Exiting..."
    fi
    if [[ "${FOUND_ASSET_GENERATION_BUILD}" == "false" ]]; then
      echo "Unable to find remote Asset Generation service build for ${COMMIT_HASH} within the timeout - you might need to manually trigger it in Cloud Build (https://console.cloud.google.com/cloud-build/triggers?project=$PROJECT_ID). Exiting..."
    fi

    if [[ "${FOUND_APP_ENGINE_BUILD}" == "false" ]] || [[ "${FOUND_DATAFLOW_BUILD}" == "false" ]] || [[ "${FOUND_ASSET_GENERATION_BUILD}" == "false" ]]; then
      run_cmd exit 1
    fi

    echo "Found remote App Engine build, proceeding to use image ${APP_ENGINE_REMOTE_BUILD_URL} for the release, tagging to ${APP_ENGINE_IMAGE_URL}"
    copy_docker_image_to_repository "${APP_ENGINE_REMOTE_BUILD_URL}" "${APP_ENGINE_IMAGE_URL}"

    echo "Found Dataflow build, proceeding to use image ${DATAFLOW_BUILD_URL} for the release, tagging to ${DATAFLOW_IMAGE_URL}"
    copy_docker_image_to_repository "${DATAFLOW_BUILD_URL}" "${DATAFLOW_IMAGE_URL}"

    echo "Found Asset Generation service build, proceeding to use image ${ASSET_GENERATION_BUILD_URL} for the release, tagging to ${ASSET_GENERATION_IMAGE_URL}"
    copy_docker_image_to_repository "${ASSET_GENERATION_BUILD_URL}" "${ASSET_GENERATION_IMAGE_URL}"
fi

if [[ -n ${PROMOTE} ]]; then
    # Update latest tag to reflect staging as well
    echo "Updating :latest tag on remote app engine docker image."
    verify_hash "$COMMIT_HASH"
    copy_docker_image_to_repository "${APP_ENGINE_IMAGE_URL}" "${APP_ENGINE_IMAGE_BASE}:latest"

    # Update latest tag to reflect staging as well
    echo "Updating :latest tag on remote dataflow docker image."
    verify_hash "$COMMIT_HASH"
    copy_docker_image_to_repository "${DATAFLOW_IMAGE_URL}" "${DATAFLOW_IMAGE_BASE}:latest"

    # Update latest tag to reflect staging as well
    echo "Updating :latest tag on remote asset generation service docker image."
    verify_hash "$COMMIT_HASH"
    copy_docker_image_to_repository "${ASSET_GENERATION_IMAGE_URL}" "${ASSET_GENERATION_IMAGE_BASE}:latest"
fi

if [[ -n ${PROMOTE} ]]; then
    verify_hash "$COMMIT_HASH"
    pre_deploy_configure_infrastructure "$PROJECT_ID" "${DOCKER_IMAGE_TAG}" "$COMMIT_HASH"
else
    echo "Skipping configuration and pipeline deploy steps for debug or no promote release build."
fi

echo "Deploying $VERSION_TAG to Looker staging project..."

FLAGS=""
[ "$PROMOTE" = "true" ] && FLAGS+="-p"
[ "$NO_PROMOTE" = "true" ] && FLAGS+="-n"
[ -n "$DEBUG_BUILD_NAME" ] && FLAGS+=" -d $DEBUG_BUILD_NAME"

if [[ -n ${PROMOTE} ]]; then
  echo "Deploying Looker version $VERSION_TAG at commit ${LOOKER_COMMIT_HASH:0:7} to $LOOKER_PROJECT_ID."
  deploy_looker_staging_version "$VERSION_TAG" "$LOOKER_PROJECT_ID"
  echo "Deployed Looker version $VERSION_TAG to $LOOKER_PROJECT_ID."
fi

if [[ -n ${PROMOTE} ]]; then
    echo "Deploy succeeded - triggering post-deploy jobs."
    verify_hash "$COMMIT_HASH"
    post_deploy_triggers "$PROJECT_ID"
else
    echo "Deploy succeeded - skipping post deploy triggers for no promote build."
fi

if [[ -z ${DEBUG_BUILD_NAME} ]]; then
  echo "Deploy succeeded - creating local Recidiviz/pulse-data tag ${VERSION_TAG}"
  verify_hash "${COMMIT_HASH}"
  run_cmd git tag -m "Version $VERSION_TAG release - $(date +'%Y-%m-%d %H:%M:%S')" "${VERSION_TAG}"

  echo "Pushing tags to remote"
  run_cmd git push origin --tags

  echo "Creating local Recidiviz/looker tag $VERSION_TAG"
  verify_hash "$LOOKER_COMMIT_HASH" "$TEMP_LOOKER_DIR"
  looker_git tag -m "Version $VERSION_TAG release - $(date +'%Y-%m-%d %H:%M:%S')" "$VERSION_TAG"

  echo "Pushing tags to remote"
  looker_git push origin --tags
fi

update_deployment_status "${DEPLOYMENT_STATUS_SUCCEEDED}" "${PROJECT_ID}" "${COMMIT_HASH:0:7}" "${VERSION_TAG}"

if [[ -n ${PROMOTE} ]]; then
  script_prompt "Have you completed all Post-Deploy tasks for this STAGING version in https://go/platform-deploy-log?"
fi
