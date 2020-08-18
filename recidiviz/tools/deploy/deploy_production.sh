#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")

read -p "Have you run any new migrations added since the last release for all prod DBs (jails, state, operations)? (y/n):" -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi

GIT_VERSION_TAG=$(echo $1 | tr '-' '.')
LAST_DEPLOYED_GIT_VERSION_TAG=$(gcloud app versions list --project=recidiviz-123 --hide-no-traffic --service=default --format=yaml | yq .id | tr -d \" | tr '-' '.')

echo "Commits since last deploy:"
git log --oneline $LAST_DEPLOYED_GIT_VERSION_TAG..$GIT_VERSION_TAG

read -p "Have you completed all Pre-Deploy tasks listed at http://go/deploy-checklist? (y/n):" -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi

echo "Fetching all tags"
git fetch --all --tags --prune

echo "Checking out tag $1"
git checkout tags/$1 -b $1

echo "Starting deploy of cron.yaml"
gcloud -q app deploy cron.yaml --project=recidiviz-123

echo "Starting task queue initialization"
pipenv run python -m recidiviz.tools.initialize_google_cloud_task_queues --project_id recidiviz-123 --google_auth_token $(gcloud auth print-access-token)

echo "Updating the BigQuery Dataflow metric table schemas to match the metric classes"
pipenv run python -m recidiviz.calculator.calculation_data_storage_manager --project_id recidiviz-123 --function_to_execute update_schemas

echo "Deploying prod-ready calculation pipelines to templates in recidiviz-123."
pipenv run python -m recidiviz.tools.deploy.deploy_pipeline_templates --project_id recidiviz-123 --templates_to_deploy production

GAE_VERSION=$(echo $1 | tr '.' '-')
STAGING_IMAGE_URL=us.gcr.io/recidiviz-staging/appengine/default.GAE_VERSION:latest
PROD_IMAGE_URL=us.gcr.io/recidiviz-123/appengine/default.GAE_VERSION:latest

echo "Starting deploy of main app"
gcloud -q container images add-tag $STAGING_IMAGE_URL $PROD_IMAGE_URL
gcloud -q app deploy prod.yaml --project=recidiviz-123 --version=GAE_VERSION --image-url=$PROD_IMAGE_URL

read -p "Have you completed all Post-Deploy tasks listed at http://go/deploy-checklist? (y/n):" -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi
