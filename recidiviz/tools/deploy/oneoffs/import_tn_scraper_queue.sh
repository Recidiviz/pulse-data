#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/terraform_oneoffs.sh

# We erroneously created this queue, but but the easiest thing to do to unblock the
# prod deploy will be to run this before deploy.
terraform_import module.scraper-region-queues[\"us_tn\"].google_cloud_tasks_queue.queue $PROJECT_ID/us-east1/us-tn-scraper-v2
