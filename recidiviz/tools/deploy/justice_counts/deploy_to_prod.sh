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

JUSTICE_COUNTS_PROJECT_ID='justice-counts-production'
PUBLISHER_CLOUD_RUN_SERVICE="publisher-web"
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
STAGING_IMAGE_BASE=us-central1-docker.pkg.dev/justice-counts-staging/publisher-and-dashboard-images/main
PROD_IMAGE_BASE=us-central1-docker.pkg.dev/justice-counts-production/publisher-and-dashboard-images/main

# Find the Docker image on staging with the specified tag.
STAGING_IMAGE_JSON=$(gcloud artifacts docker images list "${STAGING_IMAGE_BASE}" --filter="tags:${TAG}" --format=json --include-tags)

if [[ ${STAGING_IMAGE_JSON}  == "[]" ]]; then
    echo_error "No Docker images found in ${STAGING_IMAGE_BASE} with tag ${TAG}"
    run_cmd exit 1
fi

# Move the image from the staging GCP project to the production GCP project and tag it with 'latest'.
STAGING_IMAGE_URL=${STAGING_IMAGE_BASE}:${TAG}
PROD_IMAGE_URL="${PROD_IMAGE_BASE}:latest"
PROD_IMAGE_TAG="${PROD_IMAGE_BASE}:${TAG}"

echo "Moving Docker image ${STAGING_IMAGE_URL} to ${PROD_IMAGE_URL}..."
run_cmd copy_docker_image_to_repository "${STAGING_IMAGE_URL}" "${PROD_IMAGE_URL}"

echo "Tagging Docker image with ${PROD_IMAGE_TAG}..."
run_cmd gcloud artifacts docker tags add "${PROD_IMAGE_URL}" "${PROD_IMAGE_TAG}"

# Now we need to run migrations.
# As long as this Docker image was built using our Cloud Build Trigger, the first tag will always be the commit hash.
# We need the commit hash so we know what point in the code to run the migrations against.
RECIDIVIZ_DATA_TAGS=$(jq -r '.[0].tags' <<< "${STAGING_IMAGE_JSON}")

# Check if RECIDIVIZ_DATA_TAGS is a JSON array or a comma-separated string
if echo "${RECIDIVIZ_DATA_TAGS}" | jq empty 2>/dev/null; then
    # RECIDIVIZ_DATA_TAGS is a JSON array
    RECIDIVIZ_DATA_COMMIT_HASH=$(jq -r '.[0]' <<< "${RECIDIVIZ_DATA_TAGS}")
else
    # RECIDIVIZ_DATA_TAGS is a comma-separated string
    RECIDIVIZ_DATA_COMMIT_HASH=$(cut -d ',' -f 1 <<< "${RECIDIVIZ_DATA_TAGS}")
fi

echo "Checking out [${RECIDIVIZ_DATA_COMMIT_HASH}] in pulse-data..."
run_cmd git fetch origin "${RECIDIVIZ_DATA_COMMIT_HASH}"
run_cmd git checkout "${RECIDIVIZ_DATA_COMMIT_HASH}"

echo "Running migrations to head on ${JUSTICE_COUNTS_PROJECT_ID}..."
# note: don't use run_cmd here because it messes up confirmation prompts
python -m recidiviz.tools.migrations.run_migrations_to_head \
    --database JUSTICE_COUNTS \
    --project-id "${JUSTICE_COUNTS_PROJECT_ID}" \
    --skip-db-name-check

# Deploy both Publisher and Agency Dashboard
# Note: we need to manually update traffic in case we previously deployed a revision
# with no traffic (which we might do during playtesting deploys), in which case subsequent
# deploys also won't start sending traffic until traffic is manually updated via `update-traffic`.
echo "Deploying new Publisher Cloud Run revision with image ${PROD_IMAGE_URL}..."
run_cmd gcloud -q run deploy "${PUBLISHER_CLOUD_RUN_SERVICE}" \
    --project "${JUSTICE_COUNTS_PROJECT_ID}" \
    --image "${PROD_IMAGE_URL}" \
    --region "us-central1"

echo "Directing 100% of traffic to new revision..."
run_cmd gcloud -q run services update-traffic "${PUBLISHER_CLOUD_RUN_SERVICE}" \
    --project "${JUSTICE_COUNTS_PROJECT_ID}" \
    --to-latest \
    --region "us-central1"

echo "Deploying new Agency Dashboard Cloud Run revision with image ${PROD_IMAGE_URL}..."
run_cmd gcloud -q run deploy "${DASHBOARD_CLOUD_RUN_SERVICE}" \
    --project "${JUSTICE_COUNTS_PROJECT_ID}" \
    --image "${PROD_IMAGE_URL}" \
    --region "us-central1"

echo "Directing 100% of traffic to new revision..."
run_cmd gcloud -q run services update-traffic "${DASHBOARD_CLOUD_RUN_SERVICE}" \
    --project "${JUSTICE_COUNTS_PROJECT_ID}" \
    --to-latest \
    --region "us-central1"

# Update Image for Cloud Run Jobs
echo "Updating Image for Cloud Run Jobs"
run_cmd gcloud run jobs update recurring-report-creation --image "${PROD_IMAGE_URL}" --region "us-central1" --project "justice-counts-production"
run_cmd gcloud run jobs update csg-data-pull --image "${PROD_IMAGE_URL}" --region "us-central1" --project "justice-counts-production"
run_cmd gcloud run jobs update upload-reminder-email-job --image "${PROD_IMAGE_URL}" --region "us-central1" --project "justice-counts-production"
run_cmd gcloud run jobs update copy-superagency-metric-settings-to-child-agencies --image "${PROD_IMAGE_URL}" --region "us-central1" --project "justice-counts-production"

# TODO(#16325): Automatically create a new release in the justice-counts repo.

echo "Production deploy of Publisher and Agency Dashboard succeeded."
