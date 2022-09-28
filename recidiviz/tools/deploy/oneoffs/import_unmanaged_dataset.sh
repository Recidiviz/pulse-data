
#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/terraform_oneoffs.sh

terraform_import module.unmanaged_views_dataset.google_bigquery_dataset.dataset "${PROJECT_ID}/unmanaged_views"
