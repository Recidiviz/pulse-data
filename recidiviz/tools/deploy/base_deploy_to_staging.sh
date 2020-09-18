#!/usr/bin/env bash

#
# Base script for deploying a new version to staging, whether it be a cherry-pick, a standard release, or a new alpha
# version.
#

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh
source ${BASH_SOURCE_DIR}/deploy_helpers.sh

VERSION_TAG=''
DEBUG_BUILD_NAME=''
PROMOTE=''
NO_PROMOTE=''
PROMOTE_FLAGS=''

function print_usage {
    echo_error "usage: $0 -v VERSION [-p -n -d DEBUG_BUILD_NAME]"
    echo_error "  -v: Version tag to deploy (e.g. v1.2.0)"
    echo_error "  -p: Indicates that we should promote traffic to the newly deployed version. Can not be used with -n."
    echo_error "  -n: Indicates that we should not promote traffic to the newly deployed version. Can not be used with -p."
    echo_error "  -d: Name to append to the version for a debug local deploy (e.g. anna-test1)."
    run_cmd exit 1
}

while getopts "v:pnd:" flag; do
  case "${flag}" in
    v) VERSION_TAG=$(echo $OPTARG | tr '.' '-') ;;
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
run_cmd verify_can_deploy recidiviz-staging

if [[ ! -z ${PROMOTE} || ! -z ${DEBUG_BUILD_NAME} ]]; then
    pre_deploy_configure_infrastructure 'recidiviz-staging'
else
    echo "Skipping configuration and pipeline deploy steps for no promote release build."
fi

echo "Building docker image"
run_cmd docker build -t recidiviz-image .

if [[ ! -z ${DEBUG_BUILD_NAME} ]]; then
    VERSION=${VERSION_TAG}-${DEBUG_BUILD_NAME}
else
    VERSION=${VERSION_TAG}
fi

IMAGE_URL=us.gcr.io/recidiviz-staging/appengine/default.${VERSION}:latest || exit_on_fail

echo "Tagging image url [$IMAGE_URL] as recidiviz-image"
run_cmd docker tag recidiviz-image ${IMAGE_URL}

echo "Pushing image url [$IMAGE_URL]"
run_cmd docker push ${IMAGE_URL}

echo "Deploying application"
run_cmd gcloud -q app deploy ${PROMOTE_FLAGS} staging.yaml \
       --project recidiviz-staging \
       --version ${VERSION} \
       --image-url ${IMAGE_URL} \
       --verbosity=debug

if [[ ! -z ${PROMOTE} ]]; then
    echo "App deployed to \`${VERSION}\`.recidiviz-staging.appspot.com"
else
    echo "App deployed (but not promoted) to \`${VERSION}\`.recidiviz-staging.appspot.com"
fi
