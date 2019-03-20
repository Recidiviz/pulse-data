#!/bin/bash set -v
#
# Use verbose mode so that the commands are printed to Travis. Do not expand
# variables as some are secrets.

# Create encryption keys and authenticate with GAE
openssl aes-256-cbc -K $encrypted_f7b0b16e1068_key -iv $encrypted_f7b0b16e1068_iv -in client-secret-staging.json.enc -out client-secret-staging.json -d
gcloud -q auth activate-service-account recidiviz-staging@appspot.gserviceaccount.com --key-file client-secret-staging.json

# Deploy cron.yaml
gcloud -q app deploy cron.yaml --project=recidiviz-staging

# Generate and deploy queue.yaml
echo "Starting deploy of queue.yaml"
docker exec -it recidiviz pipenv run python -m recidiviz.tools.build_queue_config --environment all
docker cp recidiviz:/app/queue.yaml .
gcloud -q app deploy queue.yaml --project=recidiviz-staging

# App engine doesn't allow '.' in the version name
VERSION=$(echo $TRAVIS_TAG | tr '.' '-')

# Authorize docker to acess GCR
# use instead of 'auth configure-docker' as travis has an old gcloud
gcloud -q docker --authorize-only

# Push the docker image to GCR
IMAGE_URL=us.gcr.io/recidiviz-staging/appengine/default.$VERSION:latest
docker tag recidiviz-image $IMAGE_URL
docker push $IMAGE_URL

# Deploy application
gcloud -q app deploy staging.yaml --project recidiviz-staging --version $VERSION --image-url $IMAGE_URL
