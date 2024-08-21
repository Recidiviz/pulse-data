#!/usr/bin/env bash

: "Script to deploy the prototypes server to production.
Performs the following actions:
1) Moves the latest Docker image that is currently deployed in recidiviz-staging to recidiviz-123.
2) Deploys the image on Cloud Run.

Example usage:
./recidiviz/tools/deploy/prototypes/deploy_to_prod.sh
"

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../../script_base.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/../deploy_helpers.sh"

RECIDIVIZ_PROJECT_ID='recidiviz-123'

echo "Checking script is executing in a pipenv shell..."
run_cmd check_running_in_pipenv_shell

STAGING_IMAGE_BASE=us-central1-docker.pkg.dev/recidiviz-staging/prototypes/main
PROD_IMAGE_BASE=us-central1-docker.pkg.dev/recidiviz-123/prototypes/main

# Find the Docker image on staging with the specified tag.
STAGING_IMAGE_JSON=$(gcloud artifacts docker images list "${STAGING_IMAGE_BASE}" --filter="tags:latest" --format=json --include-tags)

if [[ ${STAGING_IMAGE_JSON}  == "[]" ]]; then
    echo_error "No Docker images found in ${STAGING_IMAGE_BASE} with tag latest"
    run_cmd exit 1
fi

# Move the image from the staging GCP project to the production GCP project and tag it with 'latest'.
STAGING_IMAGE_URL="${STAGING_IMAGE_BASE}:latest"
PROD_IMAGE_URL="${PROD_IMAGE_BASE}:latest"

echo "Moving Docker image ${STAGING_IMAGE_URL} to ${PROD_IMAGE_URL}..."
run_cmd copy_docker_image_to_repository "${STAGING_IMAGE_URL}" "${PROD_IMAGE_URL}"

# Deploy to Cloud Run.
echo "Deploying new Publisher Cloud Run revision with image ${PROD_IMAGE_URL}..."
run_cmd gcloud -q run deploy "prototypes" \
    --project "${RECIDIVIZ_PROJECT_ID}" \
    --image "${PROD_IMAGE_URL}" \
    --region "us-central1" \
    --verbosity=debug

echo "Production deploy of server succeeded."
