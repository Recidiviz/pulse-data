#!/usr/bin/env bash

: "Script to deploy both Justice Counts apps (Publisher and Agency Dashboard) to production.
Performs the following actions:
1) Moves the Docker image with the specified version in recidiviz-staging to recidiviz-123.
2) Runs migrations on recidiviz-123 to the commit that this Docker image was built on.
3) Deploys new Cloud Run revisions and allocates 100% traffic.

Example usage:
./recidiviz/tools/deploy/justice_counts/deploy_to_prod.sh -v v1.0.0
"

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../../script_base.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/../deploy_helpers.sh"

PROJECT_ID="recidiviz-123"
PUBLISHER_CLOUD_RUN_PROJECT_ID='recidiviz-123'
PUBLISHER_CLOUD_RUN_SERVICE="justice-counts-web"
DASHBOARD_CLOUD_RUN_PROJECT_ID='justice-counts-production'
DASHBOARD_CLOUD_RUN_SERVICE="agency-dashboard-web"
VERSION=''

function print_usage {
    echo_error "usage: $0 -v VERSION"
    echo_error "  -v: Version of backend + frontend to deploy (e.g. v1.0.0)."
    run_cmd exit 1
}

while getopts "v:" flag; do
  case "${flag}" in
    v) VERSION="$OPTARG" ;;
    *) print_usage
       run_cmd exit 1 ;;
  esac
done

if [[ -z ${VERSION} ]]; then
    echo_error "Missing/empty version argument"
    print_usage
    run_cmd exit 1
fi

if [[ ! ${VERSION} =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo_error "Invalid version - must be of the format vX.Y.Z"
    run_cmd exit 1
fi

echo "Checking script is executing in a pipenv shell..."
run_cmd check_running_in_pipenv_shell

echo "Checking for clean git status..."
run_cmd verify_clean_git_status

TAG=jc.${VERSION} 
STAGING_IMAGE_BASE="us.gcr.io/recidiviz-staging/justice-counts"
PROD_IMAGE_BASE="us.gcr.io/recidiviz-123/justice-counts"

# Find the Docker image on staging with the specified backend and frontend tags.
STAGING_IMAGE_JSON=$(gcloud container images list-tags --filter="tags:${TAG}" "${STAGING_IMAGE_BASE}" --format=json)

if [[ ${STAGING_IMAGE_JSON}  == "[]" ]]; then
    echo_error "No Docker images found in ${STAGING_IMAGE_BASE} with tag ${TAG}"
    run_cmd exit 1
fi

STAGING_IMAGE_DIGEST=$(jq -r '.[0].digest' <<< "${STAGING_IMAGE_JSON}")
# As long as this Docker image was built using our Cloud Build Trigger, the first tag will always be the commit hash.
RECIDIVIZ_DATA_COMMIT_HASH=$(jq -r '.[0].tags[0]' <<< "${STAGING_IMAGE_JSON}")

STAGING_IMAGE_URL="${STAGING_IMAGE_BASE}@${STAGING_IMAGE_DIGEST}"
PROD_IMAGE_URL="${PROD_IMAGE_BASE}:latest"
PROD_IMAGE_TAG="${PROD_IMAGE_BASE}:${TAG}"

echo "Moving Docker image ${STAGING_IMAGE_URL} to ${PROJECT_ID} and tagging with latest and ${PROD_IMAGE_TAG}..."
run_cmd gcloud -q container images add-tag "${STAGING_IMAGE_URL}" "${PROD_IMAGE_URL}" "${PROD_IMAGE_TAG}"

echo "Checking out [${RECIDIVIZ_DATA_COMMIT_HASH}] in pulse-data..."
run_cmd git fetch origin "${RECIDIVIZ_DATA_COMMIT_HASH}"
run_cmd git checkout "${RECIDIVIZ_DATA_COMMIT_HASH}"

echo "Running migrations to head on ${PROJECT_ID}..."
# note: don't use run_cmd here because it messes up confirmation prompts
python -m recidiviz.tools.migrations.run_migrations_to_head \
    --database JUSTICE_COUNTS \
    --project-id "${PROJECT_ID}" \
    --skip-db-name-check \

# Deploy both Publisher and Agency Dashboard
# This will deploy and also allocate traffic to the latest revision. 
# Unlike in the deploy_to_staging script, we don't have to allocate traffic separately, 
# because we currently never use the --no-traffic arg when deploying to prod.
echo "Deploying new Publisher Cloud Run revision with image ${PROD_IMAGE_URL}..."
run_cmd gcloud -q run deploy "${PUBLISHER_CLOUD_RUN_SERVICE}" \
    --project "${PUBLISHER_CLOUD_RUN_PROJECT_ID}" \
    --image "${PROD_IMAGE_URL}" \
    --region "us-central1" 

echo "Deploying new Agency Dashboard Cloud Run revision with image ${PROD_IMAGE_URL}..."
run_cmd gcloud -q run deploy "${DASHBOARD_CLOUD_RUN_SERVICE}" \
    --project "${DASHBOARD_CLOUD_RUN_PROJECT_ID}" \
    --image "${PROD_IMAGE_URL}" \
    --region "us-central1" 

# TODO(#16325): Automatically create a new release in the justice-counts repo.

echo "Production deploy of Publisher and Agency Dashboard succeeded."
