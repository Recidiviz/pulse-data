#!/usr/bin/env bash

: "Script to deploy the Justice Counts application to staging for playtesting.
Performs the following actions:
1) Runs our Cloud Build Trigger to build a Docker image off of the specified branches
   in pulse-data and justice-counts.
2) Deploys a new Cloud Run revision from this image without allocating traffic,
   but with the specified tag.

Note: If your branch includes a database migration, this script does not run the migration. 
The migration will be run when the commits from your branch are deployed to staging. 

Example usage:
./recidiviz/tools/deploy/justice_counts/deploy_for_playtesting.sh -b main -f settings-feature -a publisher -t playtesting
"

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../../script_base.sh"

PUBLISHER_CLOUD_RUN_SERVICE="publisher-web"
DASHBOARD_CLOUD_RUN_SERVICE="agency-dashboard-web"

BACKEND_BRANCH=''
FRONTEND_BRANCH=''
FRONTEND_APP=''
URL_TAG=''

function print_usage {
    echo_error "usage: $0 -b BACKEND_BRANCH -f FRONTEND_BRANCH -a FRONTEND_APP -t URL_TAG"
    echo_error "  -b: Backend branch from which to build the Docker image."
    echo_error "  -f: Frontend branch from which to build the Docker image."
    echo_error "  -a: Frontend app to deploy (either publisher or agency-dashboard)."
    echo_error "  -t: Tag to add to the deployed revision (e.g. playtesting or <yourname>test)."
    run_cmd exit 1
}

function build_and_deploy {
    run_cmd pipenv run python -m recidiviz.tools.deploy.justice_counts.run_deploy_to_playtesting_trigger \
        --backend-branch "${BACKEND_BRANCH}" \
        --frontend-branch-or-sha "${FRONTEND_BRANCH}" \
        --subdirectory "${SUBDIRECTORY}" \
        --service-name "${SERVICE_NAME}" \
        --url-tag "${URL_TAG}"
}

function deploy_publisher {
    echo "Building Docker image off of ${BACKEND_BRANCH} in pulse-data and ${FRONTEND_BRANCH} in justice-counts...
            And deploying Cloud Run Publisher revision with image ${REMOTE_IMAGE_URL} to playtesting URL ${URL_TAG}..."

    SERVICE_NAME=${PUBLISHER_CLOUD_RUN_SERVICE}
    build_and_deploy

    echo "Deploy of Publisher for playtesting succeeded."
}

function deploy_dashboard {
    echo "Building Docker image off of ${BACKEND_BRANCH} in pulse-data and ${FRONTEND_BRANCH} in justice-counts...
        And deploying new Cloud Run Agency Dashboard revision with image ${REMOTE_IMAGE_URL} to playtesting URL ${URL_TAG}..."

    SERVICE_NAME=${DASHBOARD_CLOUD_RUN_SERVICE}
    build_and_deploy
    
    echo "Deploy of Agency Dashboard for playtesting succeeded."
}

while getopts "b:f:a:t:" flag; do
  case "${flag}" in
    b) BACKEND_BRANCH="$OPTARG" ;;
    f) FRONTEND_BRANCH="$OPTARG" ;;
    a) FRONTEND_APP="$OPTARG" ;;
    t) URL_TAG="$OPTARG" ;;
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

if [[ -z ${URL_TAG} ]]; then
    echo_error "Missing/empty tag argument"
    print_usage
    run_cmd exit 1
fi

# This is where Cloud Build will put the new Docker image
SUBDIRECTORY=playtesting
REMOTE_IMAGE_BASE=us-central1-docker.pkg.dev/justice-counts-staging/publisher-and-dashboard-images/${SUBDIRECTORY}

# Look up the pulse-data commit sha used in the Docker build (supports tags in a JSON array or a comma-separated string format)
RECIDIVIZ_DATA_COMMIT_HASH=$(gcloud artifacts docker images list "${REMOTE_IMAGE_BASE}" --format=json --include-tags --sort-by=~CREATE_TIME | jq -r '.[0].tags | if type == "array" then .[0] else (split(",") | .[0]) end')

# Use that to get the URL of the built Docker image
REMOTE_IMAGE_URL=${REMOTE_IMAGE_BASE}:${RECIDIVIZ_DATA_COMMIT_HASH}

if [[ ${FRONTEND_APP} == 'publisher' ]]; then
    deploy_publisher
elif [[ ${FRONTEND_APP} == 'agency-dashboard' ]]; then
    deploy_dashboard
elif [[ ${FRONTEND_APP} == 'both' ]]; then
    deploy_publisher
    deploy_dashboard
else
    echo_error "Invalid frontend application - must be either publisher, agency-dashboard, or both"
    run_cmd exit 1
fi
