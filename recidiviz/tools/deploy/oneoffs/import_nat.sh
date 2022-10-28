#!/usr/bin/env bash
BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/deploy/oneoffs/terraform_oneoffs.sh
source "${BASH_SOURCE_DIR}/terraform_oneoffs.sh"

NAT_PREFIX="$PROJECT_ID"

if [[ "$PROJECT_ID" = 'recidiviz-123' ]]; then
    NAT_PREFIX="recidiviz-production"
    terraform_import module.nat_us_west1.google_compute_router.default "${PROJECT_ID}/us-west1/${NAT_PREFIX}-dataflow-nat-router-us-west-1"
    terraform_import module.nat_us_west1.google_compute_router_nat.default "${PROJECT_ID}/us-west1/${NAT_PREFIX}-dataflow-nat-router-us-west-1/${NAT_PREFIX}-dataflow-nat-us-west1"

    terraform_import module.nat_us_central1.google_compute_router.default "${PROJECT_ID}/us-central1/${NAT_PREFIX}-dataflow-nat-router-us-central1"
    terraform_import module.nat_us_central1.google_compute_router_nat.default "${PROJECT_ID}/us-central1/${NAT_PREFIX}-dataflow-nat-router-us-central1/${NAT_PREFIX}-dataflow-nat-us-central1"
elif [[ "$PROJECT_ID" = 'recidiviz-staging' ]]; then
    terraform_import module.nat_us_west1.google_compute_router.default "${PROJECT_ID}/us-west1/${NAT_PREFIX}-dataflow-nat-router"
    terraform_import module.nat_us_west1.google_compute_router_nat.default "${PROJECT_ID}/us-west1/${NAT_PREFIX}-dataflow-nat-router/${NAT_PREFIX}-dataflow-nat"

    terraform_import module.nat_us_central1.google_compute_router.default "${PROJECT_ID}/us-central1/${NAT_PREFIX}-dataflow-nat-router-central1"
    terraform_import module.nat_us_central1.google_compute_router_nat.default "${PROJECT_ID}/us-central1/${NAT_PREFIX}-dataflow-nat-router-central1/${NAT_PREFIX}-dataflow-nat-central1"
else
    echo_error "Unrecognized project id: ${PROJECT_ID}"
    exit 1
fi

terraform_import module.nat_us_east1.google_compute_router.default "${PROJECT_ID}/us-east1/${NAT_PREFIX}-dataflow-nat-router-us-east1"
terraform_import module.nat_us_east1.google_compute_router_nat.default "${PROJECT_ID}/us-east1/${NAT_PREFIX}-dataflow-nat-router-us-east1/${NAT_PREFIX}-dataflow-nat-us-east1"

terraform_import module.nat_us_west2.google_compute_router.default "${PROJECT_ID}/us-west2/${NAT_PREFIX}-dataflow-nat-router-us-west2"
terraform_import module.nat_us_west2.google_compute_router_nat.default "${PROJECT_ID}/us-west2/${NAT_PREFIX}-dataflow-nat-router-us-west2/${NAT_PREFIX}-dataflow-nat-us-west2"

terraform_import module.nat_us_west3.google_compute_router.default "${PROJECT_ID}/us-west3/${NAT_PREFIX}-dataflow-nat-router-us-west3"
terraform_import module.nat_us_west3.google_compute_router_nat.default "${PROJECT_ID}/us-west3/${NAT_PREFIX}-dataflow-nat-router-us-west3/${NAT_PREFIX}-dataflow-nat-us-west3"
