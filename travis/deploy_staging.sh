#!/usr/bin/env bash
source recidiviz/tools/script_base.sh

echo "Creating encryption keys"
run_cmd openssl aes-256-cbc -K ${encrypted_7ad4439b42dc_key} -iv ${encrypted_7ad4439b42dc_iv} -in client-secret-staging.json.enc -out client-secret-staging.json -d

echo "Authenticating wtih GAE"
run_cmd gcloud -q auth activate-service-account recidiviz-staging@appspot.gserviceaccount.com --key-file client-secret-staging.json

echo "Deploying cron.yaml"
run_cmd gcloud -q app deploy cron.yaml --project=recidiviz-staging

echo "Initializing task queues"
# TODO(3369): If we figure out how to properly authenticate the docker environment, we shouldn't have to pass an auth token here
run_cmd docker exec -it recidiviz pipenv run python -m recidiviz.tools.initialize_google_cloud_task_queues --project_id recidiviz-staging --google_auth_token $(gcloud auth print-access-token)

# App engine doesn't allow '.' in the version name
VERSION=$(echo ${TRAVIS_TAG} | tr '.' '-') || exit_on_fail

echo "Deploying version [$VERSION]"

# TODO(3369): Move this earlier so we can authenticate python commands properly?
echo "Authorizing docker to access GCR"
# Use this command instead of 'auth configure-docker' as travis has an old gcloud
run_cmd gcloud -q docker --authorize-only

# Push the docker image to GCR
IMAGE_URL=us.gcr.io/recidiviz-staging/appengine/default.${VERSION}:latest || exit_on_fail

echo "Tagging image url [$IMAGE_URL] as recidiviz-image"
run_cmd docker tag recidiviz-image ${IMAGE_URL}

echo "Pushing image url [$IMAGE_URL]"
run_cmd docker push ${IMAGE_URL}

echo "Deploying application"
run_cmd gcloud -q app deploy staging.yaml --project recidiviz-staging --version ${VERSION} --image-url ${IMAGE_URL}
