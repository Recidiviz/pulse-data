#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"
# shellcheck source=recidiviz/tools/postgres/script_helpers.sh
source "${BASH_SOURCE_DIR}/../postgres/script_helpers.sh"
# shellcheck source=recidiviz/tools/deploy/looker_helpers.sh
source "${BASH_SOURCE_DIR}/looker_helpers.sh"

VERSION_REGEX="^v([0-9]+)\.([0-9]+)\.([0-9]+)(-alpha.([0-9]+))?$"
SLACK_CHANNEL_ENG="GJDCVR2AY"
SLACK_CHANNEL_DEPLOYMENT_BOT="C040N4DLMA4"

DEPLOYMENT_LOG_PATH="${BASH_SOURCE_DIR}/log/deploy.log"


# Creates the log file at the log path and redirects output to it
function initialize_deployment_log {
  mkdir -p "$(dirname "${DEPLOYMENT_LOG_PATH}")"

  # Copy STDOUT and STDERR to the deployment log
  exec &> >(tee "${DEPLOYMENT_LOG_PATH}")
}

# Uploads the deployment logs to GCS, responds to a #deployment-bot Slack message with the logs
function upload_deployment_log {
  local PROJECT_ID=$1
  local GIT_HASH=$2
  local RELEASE_VERSION_TAG=$3
  local THREAD_TS=$4

  local CURRENT_TIME
  CURRENT_TIME="$(date +"%s")"

  local LOG_FILE_NAME="${RELEASE_VERSION_TAG}-${GIT_HASH}-${CURRENT_TIME}.log"
  local LOG_OBJECT_URI
  LOG_OBJECT_URI="gs://${PROJECT_ID}-deploy-logs/$(date '+%Y/%m/%d')/${LOG_FILE_NAME}"

  gsutil cp "${DEPLOYMENT_LOG_PATH}" "${LOG_OBJECT_URI}" > /dev/null 2>&1

  local MESSAGE
  # sed is used to strip ANSI codes such as [34m and [0;10m
  MESSAGE=$(
cat <<- EOM
\`\`\`
gsutil cat ${LOG_OBJECT_URI}
\`\`\`

\`\`\`
$(tail -n 30 "${DEPLOYMENT_LOG_PATH}" | sed -e $'s/\x1b\[[0-9;]*m//g')
\`\`\`
EOM
)

  deployment_bot_message "${PROJECT_ID}" \
    "${SLACK_CHANNEL_DEPLOYMENT_BOT}" \
    "${MESSAGE}" \
    "${THREAD_TS}" > /dev/null
}

# Uploads local internet speed for benchmarking deployment times
function upload_internet_speed {
  local PROJECT_ID=$1
  local RELEASE_VERSION_TAG=$2
  local THREAD_TS=$3
  # Only use this command if running deployments on a MacOS
  if [ "$(uname)" == "Darwin" ]; then
    # Only use this command if it is installed. Older versions of Mac don't have it
    if [[ $(command -v networkQuality) != "" ]]; then
      NETWORK_SPEED=$(networkQuality)
      local MESSAGE
      MESSAGE=$(cat << EOM
\`\`\`
${NETWORK_SPEED}
\`\`\`
EOM
)

      deployment_bot_message "${PROJECT_ID}" \
        "${SLACK_CHANNEL_DEPLOYMENT_BOT}" \
        "${MESSAGE}" \
        "${THREAD_TS}" > /dev/null
    fi
  fi
}

# Parses a version tag and output a space-separated string of the version regex capture groups.
# Example usage:
#    $ VERSION_PARTS=($(parse_version v1.2.0-alpha.0))
#    $ echo "MAJOR VERSION NUMBER: ${VERSION_PARTS[1]}"
function parse_version {
    VERSION=$1
    if [[ ! ${VERSION} =~ ${VERSION_REGEX} ]]
    then
        echo_error "Expected a version tag matching regex $VERSION_REGEX. Instead found [$VERSION]."
        exit 1
    fi

    echo "${BASH_REMATCH[@]}"
}

# Returns the last version tag on the given branch. Fails if that tag does not match the acceptable version regex.
# If INCLUDE_ALPHA is set, alpha versions will be included in the search.
function last_version_tag_on_branch {
    if [[ $# -lt 2 ]]; then
        echo "Usage: last_version_tag_on_branch <BRANCH> <INCLUDE_ALPHA> [LOCAL_REPO_PATH]"
        exit 1
    fi
    BRANCH=$1
    INCLUDE_ALPHA=$2
    LOCAL_REPO_PATH=${3:-$(git rev-parse --show-toplevel)}

    if [[ "${INCLUDE_ALPHA}" == "true" ]]; then
        LAST_VERSION_TAG_ON_BRANCH=$(git -C "$LOCAL_REPO_PATH" tag --merged "${BRANCH}" | sort_versions | tail -n 1) || exit_on_fail
    else
        LAST_VERSION_TAG_ON_BRANCH=$(git -C "$LOCAL_REPO_PATH" tag --merged "${BRANCH}" | grep -v alpha | sort_versions | tail -n 1) || exit_on_fail
    fi

    # Check that the version parses
    _=$(parse_version "${LAST_VERSION_TAG_ON_BRANCH}") || exit_on_fail

    echo "${LAST_VERSION_TAG_ON_BRANCH}"
}

# Returns the last deployed version tag in a given project by reading the
# `docker_image_tag` output directly out of the Terraform state file in GCS.
# Much faster than going through `terraform init` + `terraform output`; the
# bucket is unencrypted and this is a read-only diagnostic, so bypassing
# server-side state locks is fine — a stale read during an in-flight deploy
# is acceptable for "what is currently applied?".
function last_deployed_version_tag {
    PROJECT_ID=$1
    # Redirect output to stderr so output is not returned by this function but still
    # remains visible to the user.
    echo "Getting last deployed version tag from Terraform state for project ${PROJECT_ID}..." >&2

    DOCKER_IMAGE_TAG="$(gsutil cat "gs://${PROJECT_ID}-tf-state/default.tfstate" \
        | jq -r '.outputs.docker_image_tag.value')" || exit_on_fail
    echo "Retrieved last deployed version: ${DOCKER_IMAGE_TAG}" >&2
    echo "${DOCKER_IMAGE_TAG}"
}

# The Cloud Run service we use as the source of deploy history.
# `case-triage-web` exists in both recidiviz-123 and recidiviz-staging, is
# deployed on every release, and — importantly — its TF sets
# `revision.metadata.name` to `case-triage-web-${local.git_short_hash}` (see
# cloud-run.tf), so the deployed commit's short SHA is embedded right in the
# revision name.
DEPLOY_HISTORY_CLOUD_RUN_SERVICE="case-triage-web"
DEPLOY_HISTORY_CLOUD_RUN_REGION="us-central1"

# Prints up to $LIMIT most-recent revisions of the deploy-history Cloud Run
# service as "<git_short_sha>\t<created_at>" rows, newest first. The SHA is
# extracted from the revision name suffix; for older revisions that predate
# the `<service>-<hash>` naming convention, that suffix will be the
# auto-generated suffix (e.g. `00765-skz`) rather than a git hash — callers
# need to detect and handle that case.
function recent_deploy_cloud_run_revisions {
    PROJECT_ID=$1
    LIMIT=${2:-10}
    gcloud run revisions list \
        --service="${DEPLOY_HISTORY_CLOUD_RUN_SERVICE}" \
        --region="${DEPLOY_HISTORY_CLOUD_RUN_REGION}" \
        --project="${PROJECT_ID}" \
        --format=json \
        --limit="${LIMIT}" \
        | jq -r --arg prefix "${DEPLOY_HISTORY_CLOUD_RUN_SERVICE}-" \
            '.[] | "\(.metadata.name | sub($prefix; ""))\t\(.metadata.creationTimestamp)"'
}

function next_alpha_version {
    PREVIOUS_VERSION=$1
    declare -a PREVIOUS_VERSION_PARTS
    read -r -a PREVIOUS_VERSION_PARTS < <(parse_version "${PREVIOUS_VERSION}") || exit_on_fail

    MAJOR=${PREVIOUS_VERSION_PARTS[1]}
    MINOR=${PREVIOUS_VERSION_PARTS[2]}
    PATCH=${PREVIOUS_VERSION_PARTS[3]}
    ALPHA=${PREVIOUS_VERSION_PARTS[4]-}  # Optional
    ALPHA_VERSION=${PREVIOUS_VERSION_PARTS[5]-}  # Optional

    if [[ -z ${ALPHA} ]]; then
        # If the previous version was a release version, bump the minor version and build a fresh alpha version
        NEW_VERSION="v$MAJOR.$((MINOR + 1)).0-alpha.0"
    else
        # If the previous version was an alpha version, just increment alpha version
        NEW_VERSION="v$MAJOR.$MINOR.$PATCH-alpha.$((ALPHA_VERSION + 1))"
    fi

    echo "${NEW_VERSION}"
}

# Helper for deploying any infrastructure changes before we deploy a new version of the application. Requires that we
# have checked out the commit for the version that will be deployed.
function pre_deploy_configure_infrastructure {
    PROJECT=$1
    GIT_VERSION_TAG=$2
    COMMIT_HASH=$3

    echo "Deploying terraform inside Cloud Build"
    verify_hash "$COMMIT_HASH"
    run_cmd uv run python -m recidiviz.tools.deploy.cloud_build.deployment_stage_runner \
      --project-id "${PROJECT}" \
      --version-tag "${GIT_VERSION_TAG}" \
      --commit-ref "${COMMIT_HASH}" \
      --stage "CreateTerraformPlan" \
      --apply

    verify_hash "$COMMIT_HASH"
    echo "Running migrations"
    run_cmd uv run python -m recidiviz.tools.deploy.cloud_build.deployment_stage_runner \
      --project-id "${PROJECT}" \
      --version-tag "${GIT_VERSION_TAG}" \
      --commit-ref "${COMMIT_HASH}" \
      --stage "RunMigrations"

    echo "Building Cloud Functions"
    run_cmd uv run python -m recidiviz.tools.deploy.cloud_build.deployment_stage_runner \
      --project-id "${PROJECT}" \
      --version-tag "${GIT_VERSION_TAG}" \
      --commit-ref "${COMMIT_HASH}" \
      --stage "BuildCloudFunctions"
}

function copy_docker_image_to_repository {
  # Copies a Docker image from one repository to another
  # It takes two arguments which must be Container Registry or Artifact Registry image URLS
  # crane is a tool for interacting with remote images and registries. gcrane supports GCP-specifc commands
  # https://github.com/google/go-containerregistry/blob/main/cmd/gcrane/README.md
  local IMAGE_FROM=$1
  local IMAGE_TO=$2
  run_cmd docker run \
    -v ~/.config/gcloud:/.config/gcloud \
    -e GOOGLE_APPLICATION_CREDENTIALS=/.config/gcloud/application_default_credentials.json \
    --rm gcr.io/go-containerregistry/gcrane \
    cp "${IMAGE_FROM}" "${IMAGE_TO}"
}

function check_running_in_venv {
    if [[ -z "${VIRTUAL_ENV:-}" ]]; then
        echo_error "Must be running inside the virtual environment to deploy."
        echo_error "Run 'source .venv/bin/activate' or use 'uv run' prefix."
        exit 1
    fi
}

function check_python_version {
  PYTHON_VERSION=$(python -V | grep "Python " | cut -d ' ' -f 2)
  # Fetch the required Python version from pyproject.toml
  PYTHON_SCRIPT=$(cat << EOM
import tomllib
from packaging.specifiers import SpecifierSet
with open("pyproject.toml", "rb") as f:
    spec = SpecifierSet(tomllib.load(f)["project"]["requires-python"])
# Extract the minimum version from the specifier (the >= bound)
for s in spec:
    if s.operator in (">=", "=="):
        print(s.version)
        break
EOM
)
  MIN_REQUIRED_PYTHON_VERSION=$(echo -e "$PYTHON_SCRIPT" | uv run python) || exit_on_fail
  PYTHON_MAJOR_MINOR_VERSION=${PYTHON_VERSION:0:${#MIN_REQUIRED_PYTHON_VERSION}}  || exit_on_fail

  if [[ "${MIN_REQUIRED_PYTHON_VERSION}" != "${PYTHON_MAJOR_MINOR_VERSION}" ]]; then
    echo_error "Installed Python version [v${PYTHON_VERSION}] must be at least [v${MIN_REQUIRED_PYTHON_VERSION}]."
    echo_error "Please install [v$MIN_REQUIRED_PYTHON_VERSION]. "
    echo_error "See instructions at go/backend-eng-setup for how to upgrade to [$MIN_REQUIRED_PYTHON_VERSION]."
    exit 1
  fi
}

function check_psycopg2 {
  PYTHON_SCRIPT="import psycopg2"
  PSYCOPG_DEBUG=1 python -c "${PYTHON_SCRIPT}" > /dev/null 2>&1
  PSYCOPG_CONFIGURED=$?
  if [[ $PSYCOPG_CONFIGURED -eq 1 ]]; then
    echo_error "The \`psycopg2\` package is not installed correctly. It likely has misconfigured C bindings"
    echo_error "Run \`PSYCOPG_DEBUG=1 python -c '${PYTHON_SCRIPT}'\` for more information "
    exit 1
  fi
}

function check_terraform_installed {
    if [[ -z $(which terraform) ]]; then
        echo_error "The \`terraform\` package is not installed (needed to install cloud functions). To install..."
        echo_error "... on Mac:"
        echo_error "    $ brew install terraform"
        exit 1
    fi

    # Check that we're on the required version of Terraform (matches ~> constraint in terraform.tf)
    TERRAFORM_VERSION=$(terraform --version | grep "^Terraform v" | cut -d ' ' -f 2 | sed 's/v//')
    echo "Installed Terraform version: v${TERRAFORM_VERSION}"
    # Note: these version numbers should be kept in sync with the ones in Dockerfile,
    # .devcontainer/devcontainer.json, recidiviz/tools/deploy/terraform/terraform.tf, and
    # .github/workflows/ci.yml
    # The terraform config uses ~> 1.11.4 which means >= 1.11.4 and < 1.12.0
    MIN_REQUIRED_TERRAFORM_VERSION="1.11.4"
    MAX_REQUIRED_TERRAFORM_VERSION="1.12.0"

    if version_less_than "${TERRAFORM_VERSION}" "${MIN_REQUIRED_TERRAFORM_VERSION}"; then
      echo_error "Installed Terraform version [v$TERRAFORM_VERSION] must be at least [v$MIN_REQUIRED_TERRAFORM_VERSION]. "
      echo_error "Please install [v$MIN_REQUIRED_TERRAFORM_VERSION]. "
      echo_error "See instructions at go/terraform for how to upgrade to [$MIN_REQUIRED_TERRAFORM_VERSION]."
      exit 1
    fi

    if ! version_less_than "${TERRAFORM_VERSION}" "${MAX_REQUIRED_TERRAFORM_VERSION}"; then
      echo_error "Installed Terraform version [v$TERRAFORM_VERSION] must be less than [v$MAX_REQUIRED_TERRAFORM_VERSION]. "
      echo_error "The terraform config requires ~> $MIN_REQUIRED_TERRAFORM_VERSION (at least $MIN_REQUIRED_TERRAFORM_VERSION but less than $MAX_REQUIRED_TERRAFORM_VERSION)."
      echo_error "See instructions at go/terraform for how to install the correct version."
      exit 1
    fi
}

function check_jq_installed {
    if [[ -z $(which jq) ]]; then
        echo_error "The \`jq\` package is not installed (needed to run the \`yq\` command) To install..."
        echo_error "... on Mac:"
        echo_error "    $ brew install jq"
        echo_error "... on Ubuntu 18.04:"
        echo_error "    $ apt update -y && apt install -y jq"
        exit 1
    fi
}

function check_gcloud_authed {
  echo "Checking gcloud user credentials..."

  if ! gcloud auth print-access-token >/dev/null 2>&1; then
    echo "No valid user credentials; running gcloud auth login..."
    gcloud auth login --update-adc
    if ! gcloud auth print-access-token >/dev/null 2>&1; then
      echo_error "gcloud auth still failed after login attempt"
      exit 1
    fi
    echo "gcloud login successful"
    return 0
  fi

  echo "gcloud user creds look good; no login needed."
}

function verify_can_deploy {
    local PROJECT_ID
    local COMMIT_HASH
    PROJECT_ID=$1
    COMMIT_HASH=$2

    run_cmd check_gcloud_authed

    echo "Checking script is executing in a virtual environment"
    run_cmd check_running_in_venv

    echo "Checking Python is correct version"
    run_cmd check_python_version

    echo "Checking psycopg2 is installed correctly"
    run_cmd check_psycopg2

    echo "Checking Docker is installed and running"
    run_cmd check_docker_running

    echo "Checking jq is installed"
    run_cmd check_jq_installed

    echo "Checking terraform is installed"
    run_cmd check_terraform_installed

    echo "Checking uv is synced"
    if ! "${BASH_SOURCE_DIR}"/../diff_uv.sh; then
      echo "uv is not synced. Running 'uv sync --all-extras'..."
      run_cmd uv sync --all-extras
      echo "Re-checking uv sync"
      if ! "${BASH_SOURCE_DIR}"/../diff_uv.sh; then
        echo "uv still not synced after running 'uv sync --all-extras'"
        exit 1
      fi
    fi

    echo "Checking Github status checks for commit"
    run_cmd python -m recidiviz.tools.deploy.verify_github_check_statuses \
      --project_id "${PROJECT_ID}" \
      --commit_ref "${COMMIT_HASH}" \
      --prompt

    echo "Verifying Looker repo credentials"
    run_cmd verify_looker_repo_credentials

    exit_on_fail
}

function reconfigure_terraform_backend {
  PROJECT_ID=$1
  TF_STATE_PREFIX=$2
  echo "Reconfiguring Terraform backend..."
  echo "Cleaning existing Terraform state directory..."
  rm -rf "${BASH_SOURCE_DIR}/.terraform/"
  echo "Running terraform init..."
  run_cmd terraform -chdir="${BASH_SOURCE_DIR}/terraform" init \
          -backend-config "bucket=${PROJECT_ID}-tf-state" \
          -backend-config "prefix=${TF_STATE_PREFIX}" \
          -reconfigure
}



# Posts a message to the #deployment-bot slack channel
function deployment_bot_message {
  local PROJECT_ID=$1
  local CHANNEL=$2
  local MESSAGE=$3
  local AUTHORIZATION_TOKEN
  AUTHORIZATION_TOKEN=$(get_secret "${PROJECT_ID}" deploy_slack_bot_authorization_token)

  declare -a CURL_ARGS
  local CURL_ARGS=(
    --silent
    -d "text=${MESSAGE}"
    -d "channel=${CHANNEL}"
    -H "Authorization: Bearer ${AUTHORIZATION_TOKEN}"
  )

  # THREAD_TS provided at argument 4
  if [ $# -eq 4 ]; then
    CURL_ARGS+=(
      -d "thread_ts=$4"
    )
  fi


  CURL_OUTPUT=$(curl "${CURL_ARGS[@]}" -X POST https://slack.com/api/chat.postMessage)
  RESPONSE_OK=$(echo "${CURL_OUTPUT}" | jq -r .ok)

  if [ "${RESPONSE_OK}" == "true" ]; then
    echo "${CURL_OUTPUT}" | jq -r .message.ts
  else
    echo "${CURL_OUTPUT}"
    return 1
  fi
}

function post_deploy_triggers {
    local PROJECT_ID=$1
    local COMMIT_HASH=$2
    local RELEASE_VERSION_TAG=$3

    # See "Deployment lifecycle and Slack notifications" header below for an
    # explanation for the use of DEPLOYMENT_STATUS.
    DEPLOYMENT_STATUS=$DEPLOYMENT_STATUS_POST_DEPLOY_RUNNING

    echo "Triggering post-deploy tasks"
    echo "[$(date '+%Y-%m-%d %T')] Running: uv run python -m recidiviz.tools.deploy.trigger_post_deploy_tasks --project-id ${PROJECT_ID}"
    if ! run_cmd_no_exiting_no_echo uv run python -m recidiviz.tools.deploy.trigger_post_deploy_tasks --project-id "${PROJECT_ID}"; then
        notify_post_deploy_triggers_failure "$PROJECT_ID" "$COMMIT_HASH" "$RELEASE_VERSION_TAG"
    fi

    DEPLOYMENT_STATUS=$DEPLOYMENT_STATUS_SUCCEEDED
}

function notify_post_deploy_triggers_failure {
  local PROJECT_ID=$1
  local COMMIT_HASH=$2
  local RELEASE_VERSION_TAG=$3

  local WARNING_MESSAGE="⚠️ \`[${RELEASE_VERSION_TAG}]\` Deploy of \`${COMMIT_HASH}\` to \`${PROJECT_ID}\` succeeded, but the post-deploy task failed. Please investigate and re-trigger if needed."

  deployment_bot_message "${PROJECT_ID}" "${SLACK_CHANNEL_DEPLOYMENT_BOT}" "${WARNING_MESSAGE}" "${DEPLOYMENT_SUCCESS_MESSAGE_TS}" > /dev/null
}


# ============================================================================
# Deployment lifecycle and Slack notifications
# ============================================================================
#
# A deploy moves through a small state machine tracked in $DEPLOYMENT_STATUS
# (constants defined just below). Function names in [brackets] are in this
# file; reading them in order is the easiest way to follow the flow.
#
# Happy-path walkthrough:
#
#   1. [update_deployment_status STARTED] — sets status to STARTED, posts
#      the 🛳 "deploy started" Slack message, and registers
#      `trap on_deploy_exited EXIT` so we have a single funnel for whatever
#      happens next.
#
#   2. The deploy body runs (terraform apply, docker push, git tag push, ...).
#
#   3. [update_deployment_status SUCCEEDED] — sets status to SUCCEEDED, posts
#      the ✅ "successfully deployed" Slack message via
#      [deployment_bot_message], and captures the message's Slack `ts` in
#      DEPLOYMENT_SUCCESS_MESSAGE_TS so later messages can thread under it.
#
#   4. [post_deploy_triggers] — flips status to POST_DEPLOY_RUNNING and shells
#      out the Python trigger script (which calls trigger_calculation_dag, in
#      recidiviz/utils/trigger_dag_helpers.py). Python exits 0; status is
#      flipped back to SUCCEEDED.
#
#   5. Script exits cleanly. [on_deploy_exited] fires (registered in step 1),
#      sees status == SUCCEEDED, and posts nothing — the right message (the
#      ✅ from step 3) is already in Slack.
#
# Variations from the happy path (most important cases):
#
# (A) Calc DAG already running when step 4 fires.
#     - trigger_calculation_dag detects the in-progress run via
#       has_running_dag_run, triggers the new run, and *skips* the
#       update_big_query_table_schemata smoke test, because the new run will
#       queue behind the existing one and the smoke test would just time out.
#     - Python exits 0; bash sees this as a normal success and behaves
#       identically to the happy path.
#     - Before this skip existed, the smoke-test timeout would fail step 4 and
#       post a misleading ❌, even though the deploy itself was fine.
#
# (B) Post-deploy script exits non-zero (e.g. the smoke test times out for an
#     unrelated reason, calc DAG fails to trigger, ...).
#     - Step 4's `if !` branch fires [notify_post_deploy_triggers_failure],
#       which posts a ⚠️ reply threaded under the ✅ message (using
#       DEPLOYMENT_SUCCESS_MESSAGE_TS as `thread_ts`).
#     - Status is then flipped to SUCCEEDED, so step 5's trap posts nothing
#       additional. Channel readers see ✅ + 💬 ⚠️.
#
# (C) Deploy body itself fails before step 3 (terraform error, docker push
#     failure, git push rejected, ...).
#     - The script exits with status still at STARTED.
#     - Step 5's trap sees status < SUCCEEDED and posts a red ❌ "deploy
#       failed" message.
#
# (D) Script killed mid-call during step 4 (ctrl-C, OOM, SIGTERM, ...).
#     - Status was flipped to POST_DEPLOY_RUNNING in step 4 but never flipped
#       back, because the kill happened in the middle of the Python call.
#     - Step 5's trap sees POST_DEPLOY_RUNNING and calls
#       [notify_post_deploy_triggers_failure] itself, posting the same ⚠️
#       thread reply that variation (B) would post explicitly.
#
# Two design choices that may not be obvious:
#
# - Slack threading via DEPLOYMENT_SUCCESS_MESSAGE_TS: Slack identifies each
#   message by a `ts` timestamp. A follow-up message renders under another
#   message (as "1 reply" in a thread, not a separate channel post) by passing
#   the original's `ts` as `thread_ts` on the chat.postMessage call. That's
#   why we keep the success message's ts as a global: both variations (B) and
#   (D) need to thread under it.
#
# - POST_DEPLOY_RUNNING is set by direct assignment, not via
#   update_deployment_status: update_deployment_status has fat side effects
#   (Slack messages, log uploads, gcloud calls) on STARTED/SUCCEEDED. The
#   transitional value is just a breadcrumb for on_deploy_exited; there's
#   nothing for the helper to do.
DEPLOYMENT_STATUS_INITIAL=0
DEPLOYMENT_STATUS_STARTED=1
DEPLOYMENT_STATUS_SUCCEEDED=2
DEPLOYMENT_STATUS_POST_DEPLOY_RUNNING=3
DEPLOYMENT_STATUS=$DEPLOYMENT_STATUS_INITIAL

DEPLOYMENT_SUCCESS_MESSAGE_TS=""


DEPLOYMENT_STARTED_EMOJI=(🛳 🚀 ⛴ 🚢 🛸 ✈️ 🕊 🦅 ⛵ ️🚤 🛥 🛶 🚁 🛰 🚝 🚞 🚲 🛵 🛴)
DEPLOYMENT_FAILED_EMOJI=(🌨 🌊 ⛈ 🌩 🌫 🌚 🗺 🚧)
DEPLOYMENT_SUCCESS_EMOJI=(🌅 🌁 🌄 🏞 🎑 🗾 🌠 🎇 🎆 🌇 🌆 🏙 🌃 🌌 🌉 🌁 🛤)


function on_deploy_exited {
  local PROJECT_ID=$1
  local COMMIT_HASH=$2
  local RELEASE_VERSION_TAG=$3


  if [[ "${DEPLOYMENT_STATUS}" == "${DEPLOYMENT_STATUS_POST_DEPLOY_RUNNING}" ]]; then
    notify_post_deploy_triggers_failure "${PROJECT_ID}" "${COMMIT_HASH}" "${RELEASE_VERSION_TAG}"
  elif [[ "${DEPLOYMENT_STATUS}" -lt "${DEPLOYMENT_STATUS_SUCCEEDED}" ]]; then
    local EMOJI=${DEPLOYMENT_FAILED_EMOJI[$RANDOM % ${#DEPLOYMENT_FAILED_EMOJI[@]}]}
    local DEPLOYMENT_ERROR_MESSAGE="${EMOJI} \`[${RELEASE_VERSION_TAG}]\` There was an error deploying \`${COMMIT_HASH}\` to \`${PROJECT_ID}\`"
    local ERROR_MESSAGE_TS
    ERROR_MESSAGE_TS=$(deployment_bot_message "${PROJECT_ID}" "${SLACK_CHANNEL_DEPLOYMENT_BOT}" "${DEPLOYMENT_ERROR_MESSAGE}")

    upload_deployment_log "${PROJECT_ID}" "${COMMIT_HASH}" "${RELEASE_VERSION_TAG}" "${ERROR_MESSAGE_TS}"
  fi
}


function update_deployment_status {
  local NEW_DEPLOYMENT_STATUS=$1
  local PROJECT_ID=$2
  local COMMIT_HASH=$3
  local RELEASE_VERSION_TAG=$4

  DEPLOYMENT_STATUS="${NEW_DEPLOYMENT_STATUS}"
  GCLOUD_USER=$(gcloud config get-value account) || exit_on_fail
  if [ "${DEPLOYMENT_STATUS}" == "${DEPLOYMENT_STATUS_STARTED}" ]; then
    initialize_deployment_log

    EMOJI=${DEPLOYMENT_STARTED_EMOJI[$RANDOM % ${#DEPLOYMENT_STARTED_EMOJI[@]}]}
    DEPLOY_STARTED_MESSAGE="${EMOJI} \`[${RELEASE_VERSION_TAG}]\` ${GCLOUD_USER} started deploying \`${COMMIT_HASH}\` to \`${PROJECT_ID}\`"
    deployment_bot_message "${PROJECT_ID}" "${SLACK_CHANNEL_DEPLOYMENT_BOT}" "${DEPLOY_STARTED_MESSAGE}" > /dev/null

    # Register exit hook in case the deploy fails midway
    # In this case, we want to expand (interpolate) the local variables into the trap command. Disabling SC2064
    # shellcheck disable=SC2064
    trap "on_deploy_exited '${PROJECT_ID}' '${COMMIT_HASH}' '${RELEASE_VERSION_TAG}'" EXIT
  elif [ "${DEPLOYMENT_STATUS}" == "${DEPLOYMENT_STATUS_SUCCEEDED}" ]; then
      MINUTES=$((SECONDS / 60))
      EMOJI=${DEPLOYMENT_SUCCESS_EMOJI[$RANDOM % ${#DEPLOYMENT_SUCCESS_EMOJI[@]}]}
      DEPLOY_SUCCEEDED_MESSAGE="${EMOJI} \`[${RELEASE_VERSION_TAG}]\` ${GCLOUD_USER} successfully deployed to \`${PROJECT_ID}\` in ${MINUTES} minutes"

      DEPLOYMENT_SUCCESS_MESSAGE_TS=$(deployment_bot_message "${PROJECT_ID}" "${SLACK_CHANNEL_DEPLOYMENT_BOT}" "${DEPLOY_SUCCEEDED_MESSAGE}")
      upload_deployment_log "${PROJECT_ID}" "${COMMIT_HASH}" "${RELEASE_VERSION_TAG}" "${DEPLOYMENT_SUCCESS_MESSAGE_TS}"
      upload_internet_speed "${PROJECT_ID}" "${RELEASE_VERSION_TAG}" "${DEPLOYMENT_SUCCESS_MESSAGE_TS}"

      deployment_bot_message "${PROJECT_ID}" "${SLACK_CHANNEL_ENG}" "${DEPLOY_SUCCEEDED_MESSAGE}" > /dev/null
  fi
}

function validate_release_branch_changes_since_tag {
    # Validates that the release branch has no changes since the given tag.
    if [[ $# -lt 1 || $# -gt 2 ]]; then
        echo "Usage: validate_release_branch_changes_since_tag <GIT_VERSION_TAG> [LOCAL_REPO_PATH]"
        exit 1
    fi
    GIT_VERSION_TAG=$1
    LOCAL_REPO_PATH=${2:-$(git rev-parse --show-toplevel)}

    echo "Validating no new commits on release branch since ${GIT_VERSION_TAG} in ${LOCAL_REPO_PATH}..."

    read -r -a VERSION_PARTS < <(parse_version "${GIT_VERSION_TAG}")
    MAJOR_VERSION="${VERSION_PARTS[1]}"
    MINOR_VERSION="${VERSION_PARTS[2]}"
    CHANGES_SINCE_TAG=$(git -C "$LOCAL_REPO_PATH" log "tags/${GIT_VERSION_TAG}..origin/releases/v${MAJOR_VERSION}.${MINOR_VERSION}-rc" --oneline) || exit_on_fail
    if [ -n "${CHANGES_SINCE_TAG}" ]; then
        echo "There are newly-added commits in the release branch that will not be deployed in ${GIT_VERSION_TAG}:"
        echo "${CHANGES_SINCE_TAG}" | indent_output
        echo "Folks may be expecting the above changes to go out in this production deploy (did we miss a cherry-pick?)."
        script_prompt  "Would you like to continue deploying ${GIT_VERSION_TAG}?"
    else
        echo "No new commits on release branch since ${GIT_VERSION_TAG} - OK"
    fi
}
