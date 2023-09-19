#!/usr/bin/env bash

: "Script to determine what commits will be included in a deploy to production.
Pass in the version of the staging deploy that is the upcoming production candidate.

Example usage:
./recidiviz/tools/deploy/justice_counts/commits_to_be_deployed.sh -v v1.0.0
"

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../../script_base.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/../deploy_helpers.sh"

function print_usage {
    echo_error "usage: $0 -v VERSION"
    echo_error "  -v: Version to be deployed to production (e.g. v1.0.0)."
    run_cmd exit 1
}

while getopts "v:" flag; do
  case "${flag}" in
    v) VERSION="$OPTARG" ;;
    *) print_usage
       run_cmd exit 1 ;;
  esac
done

if [[ -z ${VERSION} ]]; then
    echo_error "Missing/empty version argument"
    print_usage
    run_cmd exit 1
fi

if [[ ! ${VERSION} =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo_error "Invalid version - must be of the format vX.Y.Z"
    run_cmd exit 1
fi

PROD_IMAGE_BASE="us.gcr.io/recidiviz-123/justice-counts"

# Find the Docker image currently deployed to production (i.e. with the tag 'latest')
PROD_IMAGE_JSON=$(gcloud container images list-tags --filter="tags:latest" "${PROD_IMAGE_BASE}" --format=json)

if [[ ${PROD_IMAGE_JSON}  == "[]" ]]; then
    echo_error "No Docker images found in ${PROD_IMAGE_BASE} with tag 'latest'"
    run_cmd exit 1
fi

# The first tag should be the version
CURRENT_PROD_VERSION=$(jq -r '.[0].tags[0]' <<< "${PROD_IMAGE_JSON}")

echo "Commits in pulse-data since last deploy:"
run_cmd git log --oneline "tags/${CURRENT_PROD_VERSION}..tags/jc.${VERSION}" --grep="Justice Counts"
echo ""

cd ../justice-counts || exit
echo "Commits in justice-counts since last deploy:"
run_cmd git log --oneline "tags/${CURRENT_PROD_VERSION}..tags/jc.${VERSION}"
cd ../pulse-data || exit
