#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")

echo "Fetching all tags"
git fetch --all --tags --prune

echo "Checking out tag $1"
git checkout tags/$1 -b $1

echo "Starting deploy of cron.yaml"
gcloud -q app deploy cron.yaml --project=recidiviz-123

echo "Starting task queue initialization"
pipenv run python -m recidiviz.tools.initialize_google_cloud_task_queues --project_id recidiviz-123 --google_auth_token $(gcloud auth print-access-token)

echo "Deploying prod-ready calculation pipelines to templates in recidiviz-123."
pipenv run python -m recidiviz.tools.deploy.deploy_pipeline_templates --project_id recidiviz-123 --templates_to_deploy production

VERSION=$(echo $1 | tr '.' '-')
STAGING_IMAGE_URL=us.gcr.io/recidiviz-staging/appengine/default.$VERSION:latest
PROD_IMAGE_URL=us.gcr.io/recidiviz-123/appengine/default.$VERSION:latest

echo "Starting deploy of main app"
gcloud -q container images add-tag $STAGING_IMAGE_URL $PROD_IMAGE_URL
gcloud -q app deploy prod.yaml --project=recidiviz-123 --version=$VERSION --image-url=$PROD_IMAGE_URL
