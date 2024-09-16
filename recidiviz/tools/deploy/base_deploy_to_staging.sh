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

VERSION_TAG=''
COMMIT_HASH=''
BRANCH_NAME=''
DEBUG_BUILD_NAME=''
PROMOTE=''
NO_PROMOTE=''
PROMOTE_FLAGS=''
PROJECT_ID='recidiviz-staging'

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
    p) PROMOTE_FLAGS='--promote' PROMOTE='true';;
    n) PROMOTE_FLAGS='--no-promote' NO_PROMOTE='true';;
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

update_deployment_status "${DEPLOYMENT_STATUS_STARTED}" "${PROJECT_ID}" "${COMMIT_HASH:0:7}" "${VERSION_TAG}"

if [[ -n ${DEBUG_BUILD_NAME} ]]; then
    # app engine deploy paths
    APP_ENGINE_IMAGE_BASE=us-docker.pkg.dev/$PROJECT_ID/appengine/default
    APP_ENGINE_IMAGE_URL=$APP_ENGINE_IMAGE_BASE:${DOCKER_IMAGE_TAG} || exit_on_fail

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
    run_cmd pipenv run python -m recidiviz.tools.deploy.cloud_build.deployment_stage_runner \
      --project-id "${PROJECT_ID}" \
      --version-tag "${VERSION_TAG}" \
      --commit-ref "${COMMIT_HASH}" \
      --stage "BuildImages" \
      --images appengine,asset-generation,dataflow \
      "${PROMOTE_FLAGS}"
fi

if [[ -n ${PROMOTE} ]]; then
    verify_hash "$COMMIT_HASH"
    pre_deploy_configure_infrastructure "$PROJECT_ID" "${DOCKER_IMAGE_TAG}" "$COMMIT_HASH"
else
    echo "Skipping configuration and pipeline deploy steps for debug or no promote release build."
fi

if [[ -n ${PROMOTE} ]]; then
  echo "Deploying application - default"
  run_cmd pipenv run python -m recidiviz.tools.deploy.cloud_build.deployment_stage_runner \
    --project-id "${PROJECT_ID}" \
    --version-tag "${VERSION_TAG}" \
    --commit-ref "${COMMIT_HASH}" \
    --stage "DeployAppEngine" \
    "${PROMOTE_FLAGS}"
else
  echo "Skipping deployment of App Engine for no promote build"
  if [[ -n ${DEBUG_BUILD_NAME} ]]; then
    echo "If you wish to test the debug version on App Engine, run the following:"
    echo "pipenv run python -m recidiviz.tools.deploy.cloud_build.deployment_stage_runner \\"
    echo "  --project-id ${PROJECT_ID} \\"
    echo "  --version-tag ${VERSION_TAG} \\"
    echo "  --commit-ref ${COMMIT_HASH} \\"
    echo "  --stage DeployAppEngine \\"
    echo "  --no-promote"
  fi
fi

if [[ -n ${PROMOTE} ]]; then
    echo "Deploy succeeded - triggering post-deploy jobs."
    verify_hash "$COMMIT_HASH"
    post_deploy_triggers "$PROJECT_ID"
else
    echo "Deploy succeeded - skipping post deploy triggers for no promote build."
fi

update_deployment_status "${DEPLOYMENT_STATUS_SUCCEEDED}" "${PROJECT_ID}" "${COMMIT_HASH:0:7}" "${VERSION_TAG}"
