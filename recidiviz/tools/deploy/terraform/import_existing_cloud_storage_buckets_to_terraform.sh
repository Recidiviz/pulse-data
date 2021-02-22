#!/usr/bin/env bash

# TODO(#5928): Delete this script once prod import has completed.
declare -a states=("ID-id" "MO-mo" "ND-nd" "PA-pa")
declare -a upload_testing_states=("MO-mo")

while getopts "p:d:g:" flag; do
  case "${flag}" in
    p) PROJECT_ID="$OPTARG" ;;
    d) DOCKER_TAG="$OPTARG" ;;
    g) GIT_HASH="$OPTARG" ;;
  esac
done

if [[ -z ${PROJECT_ID} ]]; then
    echo "Missing/empty project id. Provide either: recidiviz-staging, recidiviz-123."
    exit 1
fi

if [[ -z ${DOCKER_TAG} ]]; then
    echo "Missing/empty docker version tag. Provide the version (ex: v1.73.0)."
    exit 1
fi

if [[ -z ${GIT_HASH} ]]; then
    echo "Missing/empty git hash. Provide the appropriate git hash."
    exit 1
fi

if [ ${PROJECT_ID} = "recidiviz-staging" ]
then
  for state in "${states[@]}"; do
    upper="${state:0:2}"
    lower="${state:3:4}"
    echo  "[$PROJECT_ID] Importing $upper direct ingest into terraform module."
    terraform import -var 'project_id=${PROJECT_ID}' -var 'docker_image_tag=${DOCKER_TAG}' -var 'git_hash=${GIT_HASH}' -config=recidiviz/tools/deploy/terraform/ module.state_direct_ingest_buckets_and_accounts[\"US_$upper\"].google_storage_bucket.direct-ingest-bucket ${PROJECT_ID}-direct-ingest-state-us-$lower
  done
fi

if [ ${PROJECT_ID} = "recidiviz-123" ]
then
  for state in "${states[@]}"; do
    echo  "[$PROJECT_ID] Importing $upper direct ingest into terraform module."
    terraform import -var 'project_id=${PROJECT_ID}' -var 'docker_image_tag=${DOCKER_TAG}' -var 'git_hash=${GIT_HASH}' -config=recidiviz/tools/deploy/terraform/  module.state_direct_ingest_buckets_and_accounts[\"US_$upper\"].google_storage_bucket.direct-ingest-bucket ${PROJECT_ID}-direct-ingest-state-us-$lower
  done

  for state in "${upload_testing_states[@]}"; do
    upper="${state:0:2}"
    lower="${state:3:4}"
    echo  "[$PROJECT_ID] Importing $upper testing upload direct ingest into terraform module."
    terraform import -var 'project_id=${PROJECT_ID}' -var 'docker_image_tag=${DOCKER_TAG}' -var 'git_hash=${GIT_HASH}' -config=recidiviz/tools/deploy/terraform/  module.state_direct_ingest_buckets_and_accounts[\"US_$upper\"].google_storage_bucket.prod-only-testing-direct-ingest-bucket[0] ${PROJECT_ID}-direct-ingest-state-us-$lower-upload-testing
  done
fi
