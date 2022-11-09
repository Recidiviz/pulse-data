#!/usr/bin/env bash

: "Script to deploy the Justice Counts application to staging for playtesting.
Performs the following actions:
1) Runs our Cloud Build Trigger to build a Docker image off of the specified branches
   in pulse-data and justice-counts.
2) Deploys a new Cloud Run revision from this image without allocating traffic,
   but with the playtesting tag.

Example usage:
./recidiviz/tools/deploy/justice_counts/deploy_for_playtesting.sh -b main -f settings-feature -a publisher
"

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../../script_base.sh"

PROJECT_ID='recidiviz-staging'
BACKEND_BRANCH=''
FRONTEND_BRANCH=''
FRONTEND_APP=''

function print_usage {
    echo_error "usage: $0 -b BACKEND_BRANCH -f FRONTEND_BRANCH -a FRONTEND_APP"
    echo_error "  -b: Backend branch from which to build the Docker image."
    echo_error "  -f: Frontend branch from which to build the Docker image."
    echo_error "  -a: Frontend app to deploy (either publisher or agency-dashboard)."
    run_cmd exit 1
}

while getopts "b:f:a:" flag; do
  case "${flag}" in
    b) BACKEND_BRANCH="$OPTARG" ;;
    f) FRONTEND_BRANCH="$OPTARG" ;;
    a) FRONTEND_APP="$OPTARG" ;;
    *) print_usage
       run_cmd exit 1 ;;
  esac
done


if [[ -z ${BACKEND_BRANCH} ]]; then
    echo_error "Missing/empty backend branch argument"
    print_usage
    run_cmd exit 1
fi

if [[ -z ${FRONTEND_BRANCH} ]]; then
    echo_error "Missing/empty frontend branch argument"
    print_usage
    run_cmd exit 1
fi

if [[ -z ${FRONTEND_APP} ]]; then
    echo_error "Missing/empty frontend app argument"
    print_usage
    run_cmd exit 1
fi

if [[ ${FRONTEND_APP} != 'publisher' && ${FRONTEND_APP} != 'agency-dashboard' ]]; then
    echo_error "Invalid frontend application - must be either publisher or agency-dashboard"
    run_cmd exit 1
fi

# This is where Cloud Build will put the new Docker image
SUBDIRECTORY=justice-counts/playtesting/${FRONTEND_APP}
REMOTE_IMAGE_BASE=us.gcr.io/${PROJECT_ID}/${SUBDIRECTORY}

echo "Building Docker image off of ${BACKEND_BRANCH} in pulse-data and ${FRONTEND_BRANCH} in justice-counts..."
run_cmd pipenv run python -m recidiviz.tools.deploy.justice_counts.run_cloud_build_trigger \
    --backend-branch "${BACKEND_BRANCH}" \
    --frontend-branch-or-sha "${FRONTEND_BRANCH}" \
    --frontend-app "${FRONTEND_APP}" \
    --subdirectory "${SUBDIRECTORY}"

# Look up the pulse-data commit sha used in the Docker build
RECIDIVIZ_DATA_COMMIT_HASH=$(gcloud container images list-tags "${REMOTE_IMAGE_BASE}" --format=json | jq -r '.[0].tags[0]')

# Use that to get the URL of the built Docker image
REMOTE_IMAGE_URL=${REMOTE_IMAGE_BASE}:${RECIDIVIZ_DATA_COMMIT_HASH}

# TODO(#16325): If FRONTEND_APP is agency-dashboard, deploy to a different Cloud Run service
echo "Deploying new Cloud Run revision with image ${REMOTE_IMAGE_URL} to playtesting URL..."
run_cmd gcloud -q run deploy justice-counts-web \
    --project "${PROJECT_ID}" \
    --image "${REMOTE_IMAGE_URL}" \
    --region "us-central1" \
    --tag "playtesting" \
    --no-traffic

echo "Deploy of ${FRONTEND_APP} for playtesting succeeded."
