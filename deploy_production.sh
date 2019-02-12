#!/usr/bin/env bash

echo "Fetching all tags"
git fetch --all --tags --prune

echo "Checking out tag $1"
git checkout tags/$1 -b $1

echo "Starting deploy of cron.yaml"
gcloud app deploy cron.yaml --project=recidiviz-123

echo "Starting deploy of queue.yaml"
python -m recidiviz.tools.build_queue_config
gcloud app deploy queue.yaml --project=recidiviz-123

echo "Starting deploy of main app"
gcloud app deploy prod.yaml --project=recidiviz-123
