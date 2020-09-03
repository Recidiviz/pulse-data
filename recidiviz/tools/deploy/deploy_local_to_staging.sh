#!/usr/bin/env bash

deploy_name=$1
version_tag=$(echo $2 | tr '.' '-')

if [[ x"$deploy_name" == x || x"$version_tag" == x ]]; then
    echo "usage: $0 <deploy_name> <version_tag>"
    exit 1
fi

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

echo "Updating the BigQuery Dataflow metric table schemas to match the metric classes"
run_cmd pipenv run python -m recidiviz.calculator.calculation_data_storage_manager --project_id recidiviz-staging --function_to_execute update_schemas

echo "Deploying stage-only calculation pipelines to templates in recidiviz-staging."
run_cmd pipenv run python -m recidiviz.tools.deploy.deploy_pipeline_templates --project_id recidiviz-staging --templates_to_deploy staging

echo "Deploying prod-ready calculation pipelines to templates in recidiviz-staging."
run_cmd pipenv run python -m recidiviz.tools.deploy.deploy_pipeline_templates --project_id recidiviz-staging --templates_to_deploy production

echo "Initializing task queues"
run_cmd pipenv run python -m recidiviz.tools.initialize_google_cloud_task_queues --project_id recidiviz-staging --google_auth_token $(gcloud auth print-access-token)

echo "Building docker image"
run_cmd docker build -t recidiviz-image .

echo "Tagging release"
run_cmd docker tag recidiviz-image us.gcr.io/recidiviz-staging/appengine/${deploy_name}

echo "Pushing image"
run_cmd docker push us.gcr.io/recidiviz-staging/appengine/${deploy_name}

echo "Running deploy"
run_cmd gcloud -q app deploy --no-promote staging.yaml
       --project recidiviz-staging
       --version ${version_tag}-${deploy_name}
       --image-url us.gcr.io/recidiviz-staging/appengine/${deploy_name}
       --verbosity=debug

echo "App deployed (but not promoted) to \`$version_tag-$deploy_name\`.recidiviz-staging.appspot.com"
