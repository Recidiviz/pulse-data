#!/usr/bin/env bash

echo "Fetching all tags"
git fetch --all --tags --prune

echo "Checking out tag $1"
git checkout tags/$1 -b $1

echo "Starting deploy of cron.yaml"
gcloud app deploy cron.yaml --project=recidiviz-123

echo "Starting deploy of queue.yaml"
python -m recidiviz.tools.build_queue_config --environment production
gcloud app deploy queue.yaml --project=recidiviz-123

VERSION=$(echo $1 | tr '.' '-')
STAGING_IMAGE_URL=us.gcr.io/recidiviz-staging/appengine/default.$VERSION:latest
PROD_IMAGE_URL=us.gcr.io/recidiviz-123/appengine/default.$VERSION:latest

echo "Starting deploy of main app"
docker pull $STAGING_IMAGE_URL
docker tag $STAGING_IMAGE_URL $PROD_IMAGE_URL
docker push $PROD_IMAGE_URL
gcloud app deploy prod.yaml --project=recidiviz-123 --version=$VERSION --image-url=$PROD_IMAGE_URL
