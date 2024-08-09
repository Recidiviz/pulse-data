#!/usr/bin/env bash

: "Script to deploy both Justice Counts apps (Publisher and Agency Dashboard) to staging.
Performs the following actions:
1) Runs our Cloud Build Trigger to build a Docker image off of main in pulse-data and justice-counts. 
   TODO(#16325) Allow other bases besides main.
2) Runs migrations on recidiviz-staging to head of main.
3) Deploys new Cloud Run revisions and allocates 100% traffic.
4) Tags the commits (both backend and frontend) that were used in the deploy.

Example usage:
./recidiviz/tools/deploy/justice_counts/deploy_to_staging.sh
"

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../../script_base.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/../deploy_helpers.sh"
# shellcheck source=recidiviz/tools/deploy/justice_counts/get_next_version.sh
source "${BASH_SOURCE_DIR}/../justice_counts/get_next_version.sh"

JUSTICE_COUNTS_PROJECT_ID='justice-counts-staging'
PUBLISHER_CLOUD_RUN_SERVICE="publisher-web"
DASHBOARD_CLOUD_RUN_SERVICE="agency-dashboard-web"

echo "Checking script is executing in a pipenv shell..."
run_cmd check_running_in_pipenv_shell

echo "Checking for clean git status in pulse-data..."
run_cmd verify_clean_git_status

# This is where Cloud Build will put the new Docker image
SUBDIRECTORY=main
REMOTE_IMAGE_BASE=us-central1-docker.pkg.dev/justice-counts-staging/publisher-and-dashboard-images/${SUBDIRECTORY}

# Step 1: Determine most recent commit sha of Justice Counts repo

# We pass this commit sha to the Cloud Build Trigger and then use it for the rest of the script. 
# This prevents us from  getting into a weird state where a new commit is checked in during the deploy.
# note: can't use run_cmd here because of the way cd works in shell scripts
cd ../justice-counts || exit
echo "Checking for clean git status in justice-counts..."
run_cmd verify_clean_git_status
JUSTICE_COUNTS_COMMIT_HASH=$(git fetch && git rev-parse origin/main) || exit_on_fail
cd ../pulse-data || exit

# Step 2: Determine next version tag (e.g. jc.v1.55.0)

VERSION=$(./recidiviz/tools/deploy/justice_counts/get_next_version.sh | tail -n 1) || exit_on_fail
echo "Next version is ${VERSION}"

# Step 3: Build Docker image

echo "Building Docker image off of main in pulse-data and ${JUSTICE_COUNTS_COMMIT_HASH} in justice-counts..."
run_cmd pipenv run python -m recidiviz.tools.deploy.justice_counts.run_cloud_build_trigger \
    --backend-branch "main" \
    --frontend-branch-or-sha "${JUSTICE_COUNTS_COMMIT_HASH}" \
    --subdirectory "${SUBDIRECTORY}"

# Step 4: Tag Docker image with 'latest'

# Look up the pulse-data commit sha used in the Docker build (supports tags in a JSON array or a comma-separated string format)
RECIDIVIZ_DATA_COMMIT_HASH=$(gcloud artifacts docker images list "${REMOTE_IMAGE_BASE}" --format=json --include-tags --sort-by=~CREATE_TIME | jq -r '.[0].tags | if type == "array" then .[0] else (split(",") | .[0]) end')

# Use that to get the URL of the built Docker image
COMMIT_SHA_DOCKER_TAG=${REMOTE_IMAGE_BASE}:${RECIDIVIZ_DATA_COMMIT_HASH}

echo "Adding \"latest\" tag to the Docker image..."
LATEST_DOCKER_TAG=${REMOTE_IMAGE_BASE}:latest
run_cmd gcloud artifacts docker tags add "${COMMIT_SHA_DOCKER_TAG}" "${LATEST_DOCKER_TAG}"

# Step 5: Run migrations

echo "Checking out [${RECIDIVIZ_DATA_COMMIT_HASH}] in pulse-data..."
run_cmd git fetch origin "${RECIDIVIZ_DATA_COMMIT_HASH}"
run_cmd git checkout "${RECIDIVIZ_DATA_COMMIT_HASH}"

echo "Running migrations to head on ${JUSTICE_COUNTS_PROJECT_ID}..."
# note: don't use run_cmd here because it messes up confirmation prompts
python -m recidiviz.tools.migrations.run_migrations_to_head \
    --database JUSTICE_COUNTS \
    --project-id "${JUSTICE_COUNTS_PROJECT_ID}" \
    --skip-db-name-check

# Step 6a: Deploy image with 'latest' tag to Publisher Cloud Run and update traffic
# Note: we need to manually update traffic in case we previously specified --no-traffic 
# (which we might do during playtesting deploys), in which case subsequent deploys
# won't start sending traffic until traffic is manually updated via `update-traffic`.

echo "Deploying new Publisher Cloud Run revision with image ${LATEST_DOCKER_TAG}..."
run_cmd gcloud -q run deploy "${PUBLISHER_CLOUD_RUN_SERVICE}" \
    --project "${JUSTICE_COUNTS_PROJECT_ID}" \
    --image "${LATEST_DOCKER_TAG}" \
    --region "us-central1" \

echo "Directing 100% of traffic to new revision..."
run_cmd gcloud -q run services update-traffic "${PUBLISHER_CLOUD_RUN_SERVICE}" \
    --project "${JUSTICE_COUNTS_PROJECT_ID}" \
    --to-latest \
    --region "us-central1"

# Step 6b: Deploy image with 'latest' tag to Agency Dashboard Cloud Run and update traffic

echo "Deploying new Agency Dashboard Cloud Run revision with image ${LATEST_DOCKER_TAG}..."
run_cmd gcloud -q run deploy "${DASHBOARD_CLOUD_RUN_SERVICE}" \
    --project "${JUSTICE_COUNTS_PROJECT_ID}" \
    --image "${LATEST_DOCKER_TAG}" \
    --region "us-central1" \

echo "Directing 100% of traffic to new revision..."
run_cmd gcloud -q run services update-traffic "${DASHBOARD_CLOUD_RUN_SERVICE}" \
    --project "${JUSTICE_COUNTS_PROJECT_ID}" \
    --to-latest \
    --region "us-central1"

# Step 7: Tag both the Docker image and the corresponding commits in pulse-data and justice-counts

TAG=jc.${VERSION} 

echo "Creating tag [${TAG}] on [${RECIDIVIZ_DATA_COMMIT_HASH}] of pulse-data..."
run_cmd git tag -m "Justice Counts version [$VERSION] release - $(date +'%Y-%m-%d %H:%M:%S')" "${TAG}"

echo "Pushing tag [${TAG}] to remote..."
run_cmd git push origin "${TAG}"

echo "Checking out [${JUSTICE_COUNTS_COMMIT_HASH}] in justice-counts..."
cd ../justice-counts || exit
run_cmd git fetch origin "${JUSTICE_COUNTS_COMMIT_HASH}"
run_cmd git checkout "${JUSTICE_COUNTS_COMMIT_HASH}"

echo "Creating tag [${TAG}] on [${JUSTICE_COUNTS_COMMIT_HASH}] of justice-counts..."
run_cmd git tag -m "Version [$VERSION] release - $(date +'%Y-%m-%d %H:%M:%S')" "${TAG}"

echo "Pushing tag [${TAG}] to remote..."
run_cmd git push origin "${TAG}"

echo "Adding version tags to the Docker image..."
run_cmd gcloud artifacts docker tags add "${LATEST_DOCKER_TAG}" "${REMOTE_IMAGE_BASE}:${TAG}"

# Step 8: Update Image for Cloud Run Jobs
echo "Updating Image for Cloud Run Jobs"
run_cmd gcloud run jobs update recurring-report-creation --image "${LATEST_DOCKER_TAG}" --region "us-central1" --project "justice-counts-staging"
run_cmd gcloud run jobs update recurring-new-mexico-courts-dataxchange --image "${LATEST_DOCKER_TAG}" --region "us-central1" --project "justice-counts-staging"
run_cmd gcloud run jobs update csg-data-pull --image "${LATEST_DOCKER_TAG}" --region "us-central1" --project "justice-counts-staging"
run_cmd gcloud run jobs update user-permission-check --image "${LATEST_DOCKER_TAG}" --region "us-central1" --project "justice-counts-staging"
run_cmd gcloud run jobs update upload-reminder-email-job --image "${LATEST_DOCKER_TAG}" --region "us-central1" --project "justice-counts-staging"
run_cmd gcloud run jobs update copy-superagency-metric-settings-to-child-agencies --image "${LATEST_DOCKER_TAG}" --region "us-central1" --project "justice-counts-staging"
# TODO(#16325): Create release candidate branches to facilitate cherry-picks.

# Step 9: Check out main in both justice-counts repo (where we are now) and pulse-data repo,
# so we avoid being in a 'detached head' state
run_cmd git checkout main
cd ../pulse-data || exit
run_cmd git checkout main

echo "Staging deploy of Publisher and Agency Dashboard succeeded."
