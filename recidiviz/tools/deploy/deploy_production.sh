#!/usr/bin/env bash
#
# Script for deploying tagged versions to production. Must be run within the pipenv shell.
#

if [[ x"$1" == x ]]; then
    echo "usage: $0 <version_tag>"
    exit 1
fi

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

GIT_VERSION_TAG=$(echo $1 | tr '-' '.') || exit_on_fail
LAST_DEPLOYED_GIT_VERSION_TAG=$(gcloud app versions list --project=recidiviz-123 --hide-no-traffic --service=default --format=yaml | yq .id | tr -d \" | tr '-' '.') || exit_on_fail

if [[ ! ${GIT_VERSION_TAG} =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Invalid release version [$GIT_VERSION_TAG] - must match regex: v[0-9]+\.[0-9]+\.[0-9]+"
    exit 1
fi

if ! version_less_than ${LAST_DEPLOYED_GIT_VERSION_TAG} ${GIT_VERSION_TAG}; then
    echo "Deploy version [$GIT_VERSION_TAG] must be greater than last deployed tag [$LAST_DEPLOYED_GIT_VERSION_TAG]."
    exit 1
fi

echo "Beginning deploy of version [$GIT_VERSION_TAG] to production. Last deployed version: [$LAST_DEPLOYED_GIT_VERSION_TAG]."
script_prompt "Do you want to continue?"

script_prompt "Have you run any new migrations added since the last release for all prod DBs (jails, state, operations)\
 or were there no new migrations to run?"

echo "Commits since last deploy:"
run_cmd git log --oneline tags/${LAST_DEPLOYED_GIT_VERSION_TAG}..tags/${GIT_VERSION_TAG}

script_prompt "Have you completed all Pre-Deploy tasks listed at http://go/deploy-checklist?"

echo "Fetching all tags"
run_cmd git fetch --all --tags --prune

echo "Checking for clean git status"
if [[ ! -z "$(git status --porcelain)" ]]; then
    echo "Git status not clean - please commit or stash changes before retrying."
    exit 1
fi

echo "Checking out tag [$GIT_VERSION_TAG]"
if ! git checkout tags/${GIT_VERSION_TAG} -b ${GIT_VERSION_TAG}
then
    echo "Attempting to reuse existing branch $GIT_VERSION_TAG"
    run_cmd git checkout ${GIT_VERSION_TAG}
fi

echo "Starting deploy of cron.yaml"
run_cmd gcloud -q app deploy cron.yaml --project=recidiviz-123

echo "Starting task queue initialization"
run_cmd python -m recidiviz.tools.initialize_google_cloud_task_queues --project_id recidiviz-123 --google_auth_token $(gcloud auth print-access-token)

echo "Updating the BigQuery Dataflow metric table schemas to match the metric classes"
run_cmd python -m recidiviz.calculator.calculation_data_storage_manager --project_id recidiviz-123 --function_to_execute update_schemas

echo "Deploying prod-ready calculation pipelines to templates in recidiviz-123."
run_cmd python -m recidiviz.tools.deploy.deploy_pipeline_templates --project_id recidiviz-123 --templates_to_deploy production

GAE_VERSION=$(echo ${GIT_VERSION_TAG} | tr '.' '-') || exit_on_fail
STAGING_IMAGE_URL=us.gcr.io/recidiviz-staging/appengine/default.${GAE_VERSION}:latest || exit_on_fail
PROD_IMAGE_URL=us.gcr.io/recidiviz-123/appengine/default.${GAE_VERSION}:latest || exit_on_fail

echo "Starting deploy of main app"
run_cmd gcloud -q container images add-tag ${STAGING_IMAGE_URL} ${PROD_IMAGE_URL}
run_cmd gcloud -q app deploy prod.yaml --project=recidiviz-123 --version=${GAE_VERSION} --image-url=${PROD_IMAGE_URL}

script_prompt "Have you completed all Post-Deploy tasks listed at http://go/deploy-checklist?"
