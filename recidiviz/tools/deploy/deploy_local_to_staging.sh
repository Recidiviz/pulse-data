#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")

source ${BASH_SOURCE_DIR}/deploy_pipeline_helpers.sh
source ${BASH_SOURCE_DIR}/../run_commands.sh

deploy_name=$1
version_tag=$(echo $2 | tr '.' '-')

if [ x"$deploy_name" == x -o x"$version_tag" == x ]; then
    echo "usage: $0 <deploy_name> <version_tag>"
    exit 1
fi

echo "Deploying pipeline templates"
deploy_pipeline_templates_to_staging

echo "Initializing task queues"
run_cmd "pipenv run python -m recidiviz.tools.initialize_google_cloud_task_queues --project_id recidiviz-staging --google_auth_token $(gcloud auth print-access-token)"

echo "Building docker image"
run_cmd "docker build -t recidiviz-image ."

echo "Tagging release"
run_cmd "docker tag recidiviz-image us.gcr.io/recidiviz-staging/appengine/$deploy_name"

echo "Pushing image"
run_cmd "docker push us.gcr.io/recidiviz-staging/appengine/$deploy_name"

echo "Running deploy"
run_cmd "gcloud -q app deploy --no-promote staging.yaml
       --project recidiviz-staging
       --version $version_tag-$deploy_name
       --image-url us.gcr.io/recidiviz-staging/appengine/$deploy_name
       --verbosity=debug"

echo "App deployed (but not promoted) to \`$version_tag-$deploy_name\`.recidiviz-staging.appspot.com"
