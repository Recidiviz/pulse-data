#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/terraform_oneoffs.sh
terraform_import module.direct-ingest-temporary-files.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-direct-ingest-temporary-files
terraform_import module.gcslock.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-gcslock
terraform_import module.ingest-metadata.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-ingest-metadata
terraform_import module.justice-counts-ingest.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-justice-counts-ingest
terraform_import module.processed-state-aggregates.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-processed-state-aggregates
terraform_import module.public-dashboard-data.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-public-dashboard-data
terraform_import module.report-data.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-report-data
terraform_import module.report-data-archive.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-report-data-archive
terraform_import module.report-html.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-report-html
terraform_import module.report-images.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-report-images
terraform_import module.sendgrid-data.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-sendgrid-data
terraform_import module.validation-metadata.google_storage_bucket.bucket $PROJECT_ID/$PROJECT_ID-validation-metadata
