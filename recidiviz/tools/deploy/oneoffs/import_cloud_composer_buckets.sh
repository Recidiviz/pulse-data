#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/terraform_oneoffs.sh
if [[${PROJECT_ID} == "recidiviz-staging"]]; then
    terraform_import google_storage_bucket.cloud_composer_bucket $PROJECT_ID/us-central1-orchestration-v-7f491ac2-bucket
else
    terraform_import google_storage_bucket.cloud_composer_bucket $PROJECT_ID/us-central1-orchestration-v-ee86a7c2-bucket
fi
