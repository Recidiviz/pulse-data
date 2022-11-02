#!/usr/bin/env bash

: "Script to deploy the Justice Counts application to staging.
Performs the following actions:
1) Runs our Cloud Build Trigger to build a Docker image off of main in pulse-data and justice-counts. 
   TODO(#16325) Allow other bases besides main.
2) Runs migrations on recidiviz-staging to head of main.
3) Deploys a new Cloud Run revision and allocates 100% traffic.
4) Tags the commits (both backend and frontend) that were used in the deploy.

Example usage:
./recidiviz/tools/deploy/justice_counts/deploy_to_staging.sh -b v1.0.0 -f v1.1.0 -a publisher
"

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../../script_base.sh"

PROJECT_ID='recidiviz-staging'
BACKEND_VERSION=''
FRONTEND_VERSION=''
FRONTEND_APP=''

function print_usage {
    echo_error "usage: $0 -b BACKEND_VERSION -f FRONTEND_VERSION -a FRONTEND_APP"
    echo_error "  -b: Version with which to tag the backend commit (e.g. v1.0.0)."
    echo_error "  -f: Vesion with which to tag the frontend commit (e.g. v.1.1.0)."
    echo_error "  -a: Frontend app to deploy (either publisher or agency-dashboard)."
    run_cmd exit 1
}

# TODO(#16325): Automatically determine new versions by fetching latest existing tags and incrementing old versions.
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

if [[ ${FRONTEND_APP} != 'publisher' && ${FRONTEND_APP} != 'agency-dashboard' ]]; then
    echo_error "Invalid frontend application - must be either publisher or agency-dashboard"
    run_cmd exit 1
fi

echo "Checking for clean git status..."
run_cmd verify_clean_git_status

# This is where Cloud Build will put the new Docker image
SUBDIRECTORY=justice-counts/${FRONTEND_APP}
REMOTE_IMAGE_BASE=us.gcr.io/${PROJECT_ID}/${SUBDIRECTORY}

# Pass the most recent commit sha of the Justice Counts repo to the Cloud Build Trigger,
# and then use this commit sha for the rest of the script. This prevents us from 
# getting into a weird state where a new commit is checked in during the deploy.
# note: can't use run_cmd here because of the way cd works in shell scripts
cd ../justice-counts || exit
JUSTICE_COUNTS_COMMIT_HASH=$(git rev-parse origin/main) || exit_on_fail
cd ../pulse-data || exit

echo "Building Docker image off of main in pulse-data and ${JUSTICE_COUNTS_COMMIT_HASH} in justice-counts..."
run_cmd pipenv run python -m recidiviz.tools.deploy.justice_counts.run_cloud_build_trigger \
    --backend-branch "main" \
    --frontend-branch-or-sha "${JUSTICE_COUNTS_COMMIT_HASH}" \
    --frontend-app "${FRONTEND_APP}" \
    --subdirectory "${SUBDIRECTORY}"

# Look up the pulse-data commit sha used in the Docker build
RECIDIVIZ_DATA_COMMIT_HASH=$(gcloud container images list-tags "${REMOTE_IMAGE_BASE}" --format=json | jq -r '.[0].tags[0]')

# Use that to get the URL of the built Docker image
COMMIT_SHA_DOCKER_TAG=${REMOTE_IMAGE_BASE}:${RECIDIVIZ_DATA_COMMIT_HASH}

echo "Adding \"latest\" tag to the Docker image..."
LATEST_DOCKER_TAG=${REMOTE_IMAGE_BASE}:latest
run_cmd gcloud -q container images add-tag "${COMMIT_SHA_DOCKER_TAG}" "${LATEST_DOCKER_TAG}"

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

# TODO(#16325): If FRONTEND_APP is agency-dashboard, deploy to a different Cloud Run service
echo "Deploying new Cloud Run revision with image ${LATEST_DOCKER_TAG}..."
run_cmd gcloud -q run deploy justice-counts-web \
    --project "${PROJECT_ID}" \
    --image "${LATEST_DOCKER_TAG}" \
    --region "us-central1" \

# Need to manually update traffic in case we previously specified --no-traffic 
# (which we might do during playtesting deploys), in which case subsequent deploys
# won't start sending traffic until traffic is manually updated via `update-traffic`.
echo "Directing 100% of traffic to new revision..."
run_cmd gcloud -q run services update-traffic justice-counts-web \
    --to-latest \
    --region "us-central1"

BACKEND_TAG=jc.${FRONTEND_APP}.${BACKEND_VERSION} 
FRONTEND_TAG=${FRONTEND_APP}.${FRONTEND_VERSION}

echo "Creating tag [${BACKEND_TAG}] on [${RECIDIVIZ_DATA_COMMIT_HASH}] of pulse-data..."
run_cmd git tag -m "Justice Counts version [$BACKEND_VERSION] release - $(date +'%Y-%m-%d %H:%M:%S')" "${BACKEND_TAG}"

echo "Pushing tag [${BACKEND_TAG}] to remote..."
run_cmd git push origin "${BACKEND_TAG}"

echo "Checking out [${JUSTICE_COUNTS_COMMIT_HASH}] in justice-counts..."
cd ../justice-counts || exit
run_cmd git fetch origin "${JUSTICE_COUNTS_COMMIT_HASH}"
run_cmd git checkout "${JUSTICE_COUNTS_COMMIT_HASH}"

echo "Creating tag [${FRONTEND_TAG}] on [${JUSTICE_COUNTS_COMMIT_HASH}] of justice-counts..."
run_cmd git tag -m "Version [$FRONTEND_VERSION] release of ${FRONTEND_APP} - $(date +'%Y-%m-%d %H:%M:%S')" "${FRONTEND_TAG}"

echo "Pushing tag [${FRONTEND_TAG}] to remote..."
run_cmd git push origin "${FRONTEND_TAG}"

echo "Adding frontend and backend version tags to the Docker image..."
run_cmd gcloud -q container images add-tag "${LATEST_DOCKER_TAG}" "${REMOTE_IMAGE_BASE}:${FRONTEND_TAG}"
run_cmd gcloud -q container images add-tag "${LATEST_DOCKER_TAG}" "${REMOTE_IMAGE_BASE}:${BACKEND_TAG}"

# TODO(#16325): Create release candidate branches to facilitate cherry-picks.

echo "Deploy of ${FRONTEND_APP} to ${PROJECT_ID} succeeded."
