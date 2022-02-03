#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/terraform_oneoffs.sh

# Import existing us_xx_validation datasets for staging and production

terraform_import module.state_direct_ingest_buckets_and_accounts[\"US_ID\"].module.state-specific-validation-dataset.google_bigquery_dataset.dataset "${PROJECT_ID}/us_id_validation"
terraform_import module.state_direct_ingest_buckets_and_accounts[\"US_MO\"].module.state-specific-validation-dataset.google_bigquery_dataset.dataset "${PROJECT_ID}/us_mo_validation"
terraform_import module.state_direct_ingest_buckets_and_accounts[\"US_ND\"].module.state-specific-validation-dataset.google_bigquery_dataset.dataset "${PROJECT_ID}/us_nd_validation"
terraform_import module.state_direct_ingest_buckets_and_accounts[\"US_PA\"].module.state-specific-validation-dataset.google_bigquery_dataset.dataset "${PROJECT_ID}/us_pa_validation"
terraform_import module.state_direct_ingest_buckets_and_accounts[\"US_TN\"].module.state-specific-validation-dataset.google_bigquery_dataset.dataset "${PROJECT_ID}/us_tn_validation"

