#!/usr/bin/env bash

: "Script to get the version number of the latest staging deploy.

Example usage:
./recidiviz/tools/deploy/justice_counts/version_of_latest_staging_deploy.sh
"

STAGING_IMAGE_BASE="us-central1-docker.pkg.dev/justice-counts-staging/publisher-and-dashboard-images/main"
# Find the Docker image currently deployed to staging (i.e. with the tag 'latest').
# The argument 2>/dev/null suppresses logging.
STAGING_IMAGE_JSON=$(gcloud artifacts docker images list "${STAGING_IMAGE_BASE}" --filter="tags:latest" --format=json --include-tags 2>/dev/null)

if [[ ${STAGING_IMAGE_JSON}  == "[]" ]]; then
    echo_error "No Docker images found in ${STAGING_IMAGE_JSON} with tag 'latest'"
    run_cmd exit 1
fi

# The tags of a staging image look like ({git sha}, jc.v1.XXX.0, latest), so the
# second tag is the version number.
STAGING_TAGS=$(jq -r '.[0].tags' <<< "${STAGING_IMAGE_JSON}")

# Check if STAGING_TAGS is a JSON array or a comma-separated string
if echo "${STAGING_TAGS}" | jq empty 2>/dev/null; then
  # STAGING_TAGS is a JSON array
  CURRENT_STAGING_VERSION_TAG=$(jq -r '.[1]' <<< "${STAGING_TAGS}" | xargs)
else
  # STAGING_TAGS is a comma-separated string
  CURRENT_STAGING_VERSION_TAG=$(cut -d ',' -f 2 <<< "${STAGING_TAGS}" | xargs)
fi

# We also need to remove the 'jc.' prefix from the tag to get just the version number.
CURRENT_STAGING_VERSION="${CURRENT_STAGING_VERSION_TAG#jc.}"

echo "$CURRENT_STAGING_VERSION"
