#!/usr/bin/env bash

#
# Base script for deploying a new version to staging, whether it be a cherry-pick, a standard release, or a new alpha
# version.
#

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh
source ${BASH_SOURCE_DIR}/deploy_helpers.sh

VERSION_TAG=''
COMMIT_HASH=''
BRANCH_NAME=''
DEBUG_BUILD_NAME=''
PROMOTE=''
NO_PROMOTE=''
PROMOTE_FLAGS=''

function print_usage {
    echo_error "usage: $0 -v VERSION -c COMMIT_SHA [-p -n -d DEBUG_BUILD_NAME]"
    echo_error "  -v: Version tag to deploy (e.g. v1.2.0)"
    echo_error "  -c: Full SHA of the commit that is to be deployed."
    echo_error "  -b: Name of the branch from which we're deploying. (required if promoting)"
    echo_error "  -p: Indicates that we should promote traffic to the newly deployed version. Can not be used with -n."
    echo_error "  -n: Indicates that we should not promote traffic to the newly deployed version. Can not be used with -p."
    echo_error "  -d: Name to append to the version for a debug local deploy (e.g. anna-test1)."
    run_cmd exit 1
}

while getopts "b:v:c:pnd:" flag; do
  case "${flag}" in
    v) VERSION_TAG="$OPTARG" ;;
    c) COMMIT_HASH="$OPTARG" ;;
    b) BRANCH_NAME="$OPTARG" ;;
    p) PROMOTE_FLAGS='--promote' PROMOTE='true';;
    n) PROMOTE_FLAGS='--no-promote' NO_PROMOTE='true';;
    d) DEBUG_BUILD_NAME="$OPTARG" ;;
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

if [[ -z ${BRANCH_NAME} && ! -z ${PROMOTE} ]]; then
    echo_error "Missing/empty commit branch name while promoting"
    print_usage
    run_cmd exit 1
fi

if [[ (! -z ${PROMOTE} && ! -z ${NO_PROMOTE}) ||  ( -z ${PROMOTE} && -z ${NO_PROMOTE}) ]]; then
    echo_error "Must pass exactly one of either -p (promote) or -n (no-promote) flags"
    print_usage
    run_cmd exit 1
fi

if [[ ! -z ${PROMOTE} && ! -z ${DEBUG_BUILD_NAME} ]]; then
    echo_error "Debug releases must only have  -n (no-promote) option."
    print_usage
    run_cmd exit 1
fi

echo "Performing pre-deploy verification"
verify_hash $COMMIT_HASH
run_cmd verify_can_deploy recidiviz-staging

if [[ ! -z ${DEBUG_BUILD_NAME} ]]; then
    DOCKER_IMAGE_TAG=${VERSION_TAG}-${DEBUG_BUILD_NAME}
    GAE_VERSION=$(echo $VERSION_TAG | tr '.' '-')-${DEBUG_BUILD_NAME}
else
    DOCKER_IMAGE_TAG=${VERSION_TAG}
    GAE_VERSION=$(echo $VERSION_TAG | tr '.' '-')
fi

IMAGE_BASE=us.gcr.io/recidiviz-staging/appengine/default
IMAGE_URL=$IMAGE_BASE:${DOCKER_IMAGE_TAG} || exit_on_fail

REMOTE_BUILD_BASE=us.gcr.io/recidiviz-staging/appengine/build
REMOTE_BUILD_URL=$REMOTE_BUILD_BASE:${COMMIT_HASH}

if [[ ! -z ${DEBUG_BUILD_NAME} ]]; then
    # Local debug build, we'll use docker on our local machine
    echo "Building docker image"
    verify_hash $COMMIT_HASH
    export DOCKER_BUILDKIT=1
    run_cmd docker build -t recidiviz-image .

    echo "Tagging image url [$IMAGE_URL] as recidiviz-image"
    verify_hash $COMMIT_HASH
    run_cmd docker tag recidiviz-image ${IMAGE_URL}

    echo "Pushing image url [$IMAGE_URL]"
    verify_hash $COMMIT_HASH
    run_cmd docker push ${IMAGE_URL}
else
    echo "Looking for remote build for commit ${COMMIT_HASH} on branch ${BRANCH_NAME}"
    verify_hash $COMMIT_HASH
    FOUND_REMOTE_BUILD = false
    NUM_WAITING = 0
    
    while [!${FOUND_REMOTE_BUILD} && $NUM_WAITING -le 10]
    do
        existing_tags = $(gcloud container images list-tags --filter="tags:${COMMIT_HASH}" --format=json ${REMOTE_BUILD_DESTINATION})
        FOUND_REMOTE_BUILD = [[ "$existing_tags" != "[]" ]]
        if [[ !${FOUND_REMOTE_BUILD} ]]; then
            echo "Remote build for commit ${COMMIT_HASH} not found, retrying in 30s"
            NUM_WAITING=$(( $NUM_WAITING + 1 ))
            sleep 30s
        fi
    done

    if [[ !${FOUND_REMOTE_BUILD} ]]; then
        echo "Unable to find remote build for ${COMMIT_HASH} within the timeout - you might need to manually trigger it in Cloud Build. Exiting..."
        exit_on_fail
    fi

    echo "Found remote build, proceeding to use image ${REMOTE_BUILD_URL} for the release, tagging to ${IMAGE_URL}"
    run_cmd gcloud -q container images add-tag ${REMOTE_BUILD_URL} ${IMAGE_URL}

if [[ ! -z ${PROMOTE} ]]; then
    # Update latest tag to reflect staging as well
    echo "Updating :latest tag on remote docker image."
    verify_hash $COMMIT_HASH
    run_cmd gcloud -q container images add-tag ${IMAGE_URL} $IMAGE_BASE:latest
fi

if [[ ! -z ${PROMOTE} ]]; then
    echo "Running migrations for all staging instances on prod-data-client. You may be asked to provide an ssh passphrase."
    # The remote migration execution script doesn't play nice with run_cmd
    gcloud compute ssh --ssh-flag="-t" prod-data-client --command "cd pulse-data && git fetch && git checkout $BRANCH_NAME \
        && git pull && pipenv run ./recidiviz/tools/migrations/run_all_staging_migrations.sh $COMMIT_HASH"
    exit_on_fail
fi

if [[ ! -z ${PROMOTE} ]]; then
    verify_hash $COMMIT_HASH
    pre_deploy_configure_infrastructure 'recidiviz-staging' "${DOCKER_IMAGE_TAG}" "$COMMIT_HASH"
else
    echo "Skipping configuration and pipeline deploy steps for debug or no promote release build."
fi

# TODO(#3928): Migrate deploy of app engine services to terraform.
echo "Deploying application - default"
verify_hash $COMMIT_HASH
run_cmd gcloud -q app deploy ${PROMOTE_FLAGS} staging.yaml \
       --project recidiviz-staging \
       --version ${GAE_VERSION} \
       --image-url ${IMAGE_URL} \
       --verbosity=debug

if [[ -z ${DEBUG_BUILD_NAME} ]]; then
    echo "Deploying application - scrapers"
    verify_hash $COMMIT_HASH
    run_cmd gcloud -q app deploy ${PROMOTE_FLAGS} staging-scrapers.yaml \
        --project recidiviz-staging \
        --version ${GAE_VERSION} \
        --image-url ${IMAGE_URL} \
        --verbosity=debug
else
    echo "Skipping deploy of scrapers for a debug build."
fi

if [[ ! -z ${PROMOTE} ]]; then
    echo "App deployed to \`${GAE_VERSION}\`.recidiviz-staging.appspot.com"
else
    echo "App deployed (but not promoted) to \`${GAE_VERSION}\`.recidiviz-staging.appspot.com"
fi

if [[ ! -z ${PROMOTE} ]]; then
    echo "Deploy succeeded - triggering post-deploy jobs."
    verify_hash $COMMIT_HASH
    post_deploy_triggers 'recidiviz-staging'
else
    echo "Deploy succeeded - skipping post deploy triggers for no promote build."
fi
