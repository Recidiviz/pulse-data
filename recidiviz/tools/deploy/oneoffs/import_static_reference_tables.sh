#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/deploy/oneoffs/terraform_oneoffs.sh
source "${BASH_SOURCE_DIR}/terraform_oneoffs.sh"
terraform_import module.static_reference_tables.google_bigquery_dataset.dataset "${PROJECT_ID}/static_reference_tables"
