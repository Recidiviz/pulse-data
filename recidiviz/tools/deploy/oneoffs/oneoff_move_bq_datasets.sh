#!/usr/bin/env bash
# Move BigQuery Datasets to use new module resource naming

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source "${BASH_SOURCE_DIR}"/terraform_oneoffs.sh


# Move each of the us_xx_scratch datasets
state_codes=("US_ID" "US_ME" "US_MI" "US_MO" "US_ND" "US_PA" "US_TN")

for state_code in "${state_codes[@]}"
do
  terraform_mv module.state_direct_ingest_buckets_and_accounts["$state_code"].google_bigquery_dataset.state_specific_scratch_dataset[0] \
    module.state_direct_ingest_buckets_and_accounts["$state_code"].module.state-specific-scratch-dataset[0].google_bigquery_dataset.dataset
done

# Move validation_results dataset
terraform_mv google_bigquery_dataset.validation_results module.validation_results_dataset.google_bigquery_dataset.dataset

# Move external_reference dataset
terraform_mv google_bigquery_dataset.external_reference module.external_reference_dataset.google_bigquery_dataset.dataset
