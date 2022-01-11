#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/terraform_oneoffs.sh
terraform_import module.covid-dashboard-data.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-covid-dashboard-data
terraform_import module.configs.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-configs
terraform_import module.dashboard-data.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-dashboard-data
terraform_import module.dataflow-templates.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-dataflow-templates
terraform_import module.dbexport.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-dbexport
terraform_import module.direct-ingest-county-storage.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-direct-ingest-county-storage
