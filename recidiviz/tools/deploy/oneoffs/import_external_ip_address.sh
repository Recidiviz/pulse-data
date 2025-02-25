#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/deploy/oneoffs/terraform_oneoffs.sh
source "${BASH_SOURCE_DIR}/terraform_oneoffs.sh"

terraform_import google_compute_address.external_system_outbound_requests "${PROJECT_ID}/us-central1/external-system-outbound-requests"
