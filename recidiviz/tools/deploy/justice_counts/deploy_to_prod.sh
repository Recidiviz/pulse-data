#!/usr/bin/env bash

: "Script to deploy the Justice Counts application to production.
Performs the following actions:
1) Moves the Docker image with the specified version tags in recidiviz-staging to recidiviz-123.
2) Runs migrations on recidiviz-123 to the commit that this Docker image was built on.
3) Deploys a new Cloud Run revision and allocates 100% traffic.

Example usage:
./recidiviz/tools/deploy/justice_counts/deploy_to_prod.sh -b v1.0.0 -f v1.1.0 -a publisher
"

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../../script_base.sh"

PROJECT_ID='recidiviz-123'
BACKEND_VERSION=''
FRONTEND_VERSION=''
FRONTEND_APP=''
CLOUD_RUN_SERVICE=''

function print_usage {
    echo_error "usage: $0 -b BACKEND_VERSION -f FRONTEND_VERSION -a FRONTEND_APP"
    echo_error "  -b: Version of backend to deploy (e.g. v1.0.0)."
    echo_error "  -f: Version of frontend to deploy (e.g. v.1.1.0)."
    echo_error "  -a: Frontend app to deploy (either publisher or agency-dashboard)."
    run_cmd exit 1
}

while getopts "b:f:a:" flag; do
  case "${flag}" in
    b) BACKEND_VERSION="$OPTARG" ;;
    f) FRONTEND_VERSION="$OPTARG" ;;
    a) FRONTEND_APP="$OPTARG" ;;
    *) print_usage
       run_cmd exit 1 ;;
  esac
done

if [[ -z ${BACKEND_VERSION} ]]; then
    echo_error "Missing/empty backend version argument"
    print_usage
    run_cmd exit 1
fi

if [[ -z ${FRONTEND_VERSION} ]]; then
    echo_error "Missing/empty frontend version argument"
    print_usage
    run_cmd exit 1
fi

if [[ -z ${FRONTEND_APP} ]]; then
    echo_error "Missing/empty frontend app argument"
    print_usage
    run_cmd exit 1
fi

if [[ ! ${BACKEND_VERSION} =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo_error "Invalid backend version - must be of the format vX.Y.Z"
    run_cmd exit 1
fi

if [[ ! ${FRONTEND_VERSION} =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo_error "Invalid frontend version - must be of the format vX.Y.Z"
    run_cmd exit 1
fi


if [[ ${FRONTEND_APP} == 'publisher' ]]; then
    CLOUD_RUN_SERVICE="justice-counts-web"
elif [[ ${FRONTEND_APP} == 'agency-dashboard' ]]; then
    CLOUD_RUN_SERVICE="justice-counts-agency-dashboard-web"
else
    echo_error "Invalid frontend application - must be either publisher or agency-dashboard"
    run_cmd exit 1
fi

echo "Checking for clean git status..."
run_cmd verify_clean_git_status

BACKEND_TAG=jc.${FRONTEND_APP}.${BACKEND_VERSION} 
FRONTEND_TAG=${FRONTEND_APP}.${FRONTEND_VERSION}
STAGING_IMAGE_BASE="us.gcr.io/recidiviz-staging/justice-counts/${FRONTEND_APP}"
PROD_IMAGE_BASE="us.gcr.io/${PROJECT_ID}/justice-counts/${FRONTEND_APP}"

# Find the Docker image on staging with the specified backend and frontend tags.
STAGING_IMAGE_JSON=$(gcloud container images list-tags --filter="tags:${BACKEND_TAG} AND tags:${FRONTEND_TAG}" "${STAGING_IMAGE_BASE}" --format=json)

if [[ ${STAGING_IMAGE_JSON}  == "[]" ]]; then
    echo_error "No Docker images found in ${STAGING_IMAGE_BASE} with tags ${BACKEND_TAG} and ${FRONTEND_TAG}"
    run_cmd exit 1
fi

STAGING_IMAGE_DIGEST=$(jq -r '.[0].digest' <<< "${STAGING_IMAGE_JSON}")
# As long as this Docker image was built using our Cloud Build Trigger, the first tag will always be the commit hash.
RECIDIVIZ_DATA_COMMIT_HASH=$(jq -r '.[0].tags[0]' <<< "${STAGING_IMAGE_JSON}")

STAGING_IMAGE_URL="${STAGING_IMAGE_BASE}@${STAGING_IMAGE_DIGEST}"
PROD_IMAGE_URL="${PROD_IMAGE_BASE}:latest"
PROD_IMAGE_FRONTEND_TAG="${PROD_IMAGE_BASE}:${FRONTEND_TAG}"
PROD_IMAGE_BACKEND_TAG="${PROD_IMAGE_BASE}:${BACKEND_TAG}"

echo "Moving Docker image ${STAGING_IMAGE_URL} to ${PROJECT_ID} and tagging with latest, ${PROD_IMAGE_FRONTEND_TAG}, and ${PROD_IMAGE_BACKEND_TAG}..."
run_cmd_no_exiting gcloud -q container images add-tag "${STAGING_IMAGE_URL}" "${PROD_IMAGE_URL}" "${PROD_IMAGE_FRONTEND_TAG}" "${PROD_IMAGE_BACKEND_TAG}"

echo "Checking out [${RECIDIVIZ_DATA_COMMIT_HASH}] in pulse-data..."
run_cmd git fetch origin "${RECIDIVIZ_DATA_COMMIT_HASH}"
run_cmd git checkout "${RECIDIVIZ_DATA_COMMIT_HASH}"

echo "Running migrations to head on ${PROJECT_ID}..."
# note: don't use run_cmd here because it messes up confirmation prompts
python -m recidiviz.tools.migrations.run_migrations_to_head \
    --database JUSTICE_COUNTS \
    --project-id "${PROJECT_ID}" \
    --skip-db-name-check \
    --using-proxy

# This will deploy and also allocate traffic to the latest revision. 
# Unlike in the deploy_to_staging script, we don't have to allocate traffic separately, 
# because we currently never use the --no-traffic arg when deploying to prod.
echo "Deploying new Cloud Run revision with image ${PROD_IMAGE_URL}..."
run_cmd gcloud -q run deploy "${CLOUD_RUN_SERVICE}" \
    --project "${PROJECT_ID}" \
    --image "${PROD_IMAGE_URL}" \
    --region "us-central1" 

# TODO(#16325): Automatically create a new release in the justice-counts repo.

echo "Deploy of ${FRONTEND_APP} to ${PROJECT_ID} succeeded."
