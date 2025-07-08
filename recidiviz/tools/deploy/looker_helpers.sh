#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"
# shellcheck source=recidiviz/tools/postgres/script_helpers.sh
source "${BASH_SOURCE_DIR}/../postgres/script_helpers.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/deploy_helpers.sh"

TEMP_LOOKER_DIR="/tmp/looker"
LOOKER_REPO_URL="https://github.com/Recidiviz/looker.git"

RECIDIVIZ_STAGING_PROJECT_ID="recidiviz-staging"
RECIDIVIZ_123_PROJECT_ID="recidiviz-123"

LOOKER_STAGING_PROJECT_ID="recidiviz-looker-staging"
LOOKER_PROD_PROJECT_ID="recidiviz-looker-123"

function looker_git {
  # Run git commands in the temporary cloned looker repo directory.
  run_cmd git -C "$TEMP_LOOKER_DIR" "$@"
}

function clone_looker_repo_to_temp_dir {
  # Clones the Looker repo into a temporary directory if it does not already exist.
  if [[ -z $(ls "$TEMP_LOOKER_DIR" 2>/dev/null) ]]; then
    run_cmd git clone "$LOOKER_REPO_URL" "$TEMP_LOOKER_DIR"
  fi
}

function deploy_looker_version {
  # Deploys the specified version tag to the specified Looker project.
  # This function requires the Looker deploy webhook secret to be set in the Recidiviz project secrets.
  if [[ $# -ne 3 ]]; then
    echo "Usage: deploy_looker_version <VERSION_TAG> <LOOKER_PROJECT_ID> <RECIDIVIZ_PROJECT_ID>"
    exit 1
  fi
  VERSION_TAG=$1
  LOOKER_PROJECT_ID=$2
  RECIDIVIZ_PROJECT_ID=$3

  LOOKER_DEPLOY_SECRET=$(get_secret "$RECIDIVIZ_PROJECT_ID" looker_deploy_webhook_secret) || exit_on_fail

  DEPLOY_URL="https://recidiviz.cloud.looker.com/webhooks/projects/${LOOKER_PROJECT_ID}/deploy/ref/${VERSION_TAG}"

  RESPONSE=$(curl -s -H "X-Looker-Deploy-Secret: ${LOOKER_DEPLOY_SECRET}" "$DEPLOY_URL") || exit_on_fail

  if echo "$RESPONSE" | jq -e '.operations[] | select(.error == true)' > /dev/null; then
    echo "Looker deployment failed with error response: $RESPONSE."
    exit 1
  fi
}

function deploy_looker_prod_version {
  # Deploys the specified version tag to the Looker production project.
  if [[ $# -ne 2 ]]; then
    echo "Usage: deploy_looker_prod_version <VERSION_TAG> <LOOKER_PROJECT_ID>"
    exit 1
  fi
  VERSION_TAG=$1
  LOOKER_PROJECT_ID=$2

  deploy_looker_version "$VERSION_TAG" "$LOOKER_PROJECT_ID" "$RECIDIVIZ_123_PROJECT_ID"
}

function deploy_looker_staging_version {
  # Deploys the specified version tag to the Looker staging project.
  if [[ $# -ne 2 ]]; then
    echo "Usage: deploy_looker_staging_version <VERSION_TAG> <LOOKER_PROJECT_ID>"
    exit 1
  fi
  VERSION_TAG=$1
  LOOKER_PROJECT_ID=$2

  deploy_looker_version "$VERSION_TAG" "$LOOKER_PROJECT_ID" "$RECIDIVIZ_STAGING_PROJECT_ID"
}

function create_looker_release_branch {
  # Creates a new release branch based on the provided commit hash in the Looker repo, updates the manifest
  # and model files to the prod looker project, and tags the commit.
  if [[ $# -ne 3 ]]; then
    echo "Usage: create_looker_release_branch <NEW_RELEASE_BRANCH> <RELEASE_VERSION_TAG> <COMMIT_HASH>"
    exit 1
  fi
  NEW_RELEASE_BRANCH=$1
  RELEASE_VERSION_TAG=$2
  COMMIT_HASH=$3

  verify_hash "$COMMIT_HASH" "$TEMP_LOOKER_DIR"

  if git -C "$TEMP_LOOKER_DIR" rev-parse --verify origin/"${NEW_RELEASE_BRANCH}" >/dev/null 2>&1; then
    echo "Error: Branch ${NEW_RELEASE_BRANCH} already exists on origin."
    exit 1
  fi

  looker_git checkout -b "${NEW_RELEASE_BRANCH}"

  # 1. Update manifest.lkml
  run_cmd sed -i'' "s/value: \"$LOOKER_STAGING_PROJECT_ID\"/value: \"$LOOKER_PROD_PROJECT_ID\"/" "${TEMP_LOOKER_DIR}/manifest.lkml"
  grep -q "value: \"$LOOKER_PROD_PROJECT_ID\"" "${TEMP_LOOKER_DIR}/manifest.lkml" || {
    echo "Error: Failed to update manifest.lkml with prod project ID" >&2
    exit 1
  }

  # 2. Rename model file
  run_cmd mv "${TEMP_LOOKER_DIR}/models/${LOOKER_STAGING_PROJECT_ID}.model.lkml" "${TEMP_LOOKER_DIR}/models/${LOOKER_PROD_PROJECT_ID}.model.lkml" || {
    echo "Error: Failed to rename model file" >&2
    exit 1
  }

  # 3. Update connection name in the renamed model file
  run_cmd sed -i "" "s/connection: \"${RECIDIVIZ_STAGING_PROJECT_ID}\"/connection: \"${RECIDIVIZ_123_PROJECT_ID}\"/" "${TEMP_LOOKER_DIR}/models/${LOOKER_PROD_PROJECT_ID}.model.lkml"
  grep -q "connection: \"${RECIDIVIZ_123_PROJECT_ID}\"" "${TEMP_LOOKER_DIR}/models/${LOOKER_PROD_PROJECT_ID}.model.lkml" || {
    echo "Error: Failed to update connection name in model file" >&2
    exit 1
  }

  looker_git add -A
  looker_git commit -m "[DEPLOY] Cut release candidate ${RELEASE_VERSION_TAG}"

  # Delete local tag if it exists
  if git -C "$TEMP_LOOKER_DIR" rev-parse "${RELEASE_VERSION_TAG}" >/dev/null 2>&1; then
    looker_git tag -d "${RELEASE_VERSION_TAG}"
    looker_git push origin ":refs/tags/${RELEASE_VERSION_TAG}" || true
  fi
  # Point the tag to the current commit
  looker_git tag -m "Version $RELEASE_VERSION_TAG release - $(date +'%Y-%m-%d %H:%M:%S')" "${RELEASE_VERSION_TAG}"

  looker_git push --set-upstream origin "${NEW_RELEASE_BRANCH}"
  looker_git push origin --tags
}
