#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/terraform_oneoffs.sh

# Import existing us_xx_validation datasets for staging and production

state_codes=("us_id" "us_mo" "us_nd" "us_pa" "us_tn")

for state_code in "${state_codes[@]}"
do
terraform_import module.state_direct_ingest_buckets_and_accounts[\"$state_code\"].module.state-specific-validation-dataset.google_bigquery_dataset.dataset "${PROJECT_ID}/${state_code}_validation"
done
