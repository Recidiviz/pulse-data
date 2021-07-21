#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/terraform_oneoffs.sh

terraform_import module.direct-ingest-state-storage.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-direct-ingest-state-storage
