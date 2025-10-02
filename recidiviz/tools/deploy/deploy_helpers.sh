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

# Returns the last deployed version tag in a given project
function last_deployed_version_tag {
    PROJECT_ID=$1
    reconfigure_terraform_backend "${PROJECT_ID}" "" > /dev/null

    DOCKER_IMAGE_TAG="$(terraform -chdir="${BASH_SOURCE_DIR}/terraform" output -raw docker_image_tag)"
    echo "${DOCKER_IMAGE_TAG}"
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
    run_cmd pipenv run python -m recidiviz.tools.deploy.cloud_build.deployment_stage_runner \
      --project-id "${PROJECT}" \
      --version-tag "${GIT_VERSION_TAG}" \
      --commit-ref "${COMMIT_HASH}" \
      --stage "CreateTerraformPlan" \
      --apply

    verify_hash "$COMMIT_HASH"
    echo "Running migrations asynchronously"
    run_cmd pipenv run python -m recidiviz.tools.deploy.cloud_build.deployment_stage_runner \
      --project-id "${PROJECT}" \
      --version-tag "${GIT_VERSION_TAG}" \
      --commit-ref "${COMMIT_HASH}" \
      --stage "RunMigrations" \
      --execute-async

    echo "Building Cloud Functions asynchronously"
    run_cmd pipenv run python -m recidiviz.tools.deploy.cloud_build.deployment_stage_runner \
      --project-id "${PROJECT}" \
      --version-tag "${GIT_VERSION_TAG}" \
      --commit-ref "${COMMIT_HASH}" \
      --stage "BuildCloudFunctions" \
      --execute-async
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

function check_running_in_pipenv_shell {
    if [[ -z $(printenv PIPENV_ACTIVE) ]]; then
        echo_error "Must be running inside the pipenv shell to deploy."
        exit 1
    fi
}

function check_python_version {
  PYTHON_VERSION=$(python -V | grep "Python " | cut -d ' ' -f 2)
  # Fetch the required Python version from the Pipfile
  PYTHON_SCRIPT=$(cat << EOM
import tomllib
with open("Pipfile", "r", encoding="utf-8") as f:
  config = tomllib.loads(f.read())
  print(config['requires']['python_version'])
EOM
)
  MIN_REQUIRED_PYTHON_VERSION=$(echo -e "$PYTHON_SCRIPT" | python) || exit_on_fail
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

    # Check that we're on at least the minimum version of Terraform
    TERRAFORM_VERSION=$(terraform --version | grep "^Terraform v" | cut -d ' ' -f 2 | sed 's/v//')
    # Note: this verison number should be kept in sync with the ones in Dockerfile,
    # .devcontainer/devcontainer.json, recidiviz/tools/deploy/terraform/terraform.tf, and
    # .github/workflows/ci.yml
    MIN_REQUIRED_TERRAFORM_VERSION="1.11.4"

    if version_less_than "${TERRAFORM_VERSION}" "${MIN_REQUIRED_TERRAFORM_VERSION}"; then
      echo_error "Installed Terraform version [v$TERRAFORM_VERSION] must be at least [v$MIN_REQUIRED_TERRAFORM_VERSION]. "
      echo_error "Please install [v$MIN_REQUIRED_TERRAFORM_VERSION]. "
      echo_error "See instructions at go/terraform for how to upgrade to [$MIN_REQUIRED_TERRAFORM_VERSION]."
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

function verify_can_deploy {
    local PROJECT_ID
    local COMMIT_HASH
    PROJECT_ID=$1
    COMMIT_HASH=$2

    echo "Checking script is executing in a pipenv shell"
    run_cmd check_running_in_pipenv_shell

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

    echo "Checking pipenv is synced"
    if ! "${BASH_SOURCE_DIR}"/../diff_pipenv.sh; then
      echo "Pipenv is not synced. Running 'pipenv sync --dev'..."
      run_cmd pipenv sync --dev
      echo "Re-checking pipenv sync"
      if ! "${BASH_SOURCE_DIR}"/../diff_pipenv.sh; then
        echo "Pipenv still not synced after running 'pipenv sync --dev'"
        exit 1
      fi
    fi

    echo "Checking Github status checks for commit"
    python -m recidiviz.tools.deploy.verify_github_check_statuses \
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
  rm -rf "${BASH_SOURCE_DIR}/.terraform/"
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
    PROJECT=$1

    echo "Triggering post-deploy tasks"

    run_cmd pipenv run python -m recidiviz.tools.deploy.trigger_post_deploy_tasks --project-id "${PROJECT}"
}


DEPLOYMENT_STATUS_INITIAL=0
DEPLOYMENT_STATUS_STARTED=1
DEPLOYMENT_STATUS_SUCCEEDED=2
DEPLOYMENT_STATUS=$DEPLOYMENT_STATUS_INITIAL


DEPLOYMENT_STARTED_EMOJI=(🛳 🚀 ⛴ 🚢 🛸 ✈️ 🕊 🦅 ⛵ ️🚤 🛥 🛶 🚁 🛰 🚝 🚞 🚲 🛵 🛴)
DEPLOYMENT_FAILED_EMOJI=(🌨 🌊 ⛈ 🌩 🌫 🌚 🗺 🚧)
DEPLOYMENT_SUCCESS_EMOJI=(🌅 🌁 🌄 🏞 🎑 🗾 🌠 🎇 🎆 🌇 🌆 🏙 🌃 🌌 🌉 🌁 🛤)


function on_deploy_exited {
  local PROJECT_ID=$1
  local COMMIT_HASH=$2
  local RELEASE_VERSION_TAG=$3


  if [[ "${DEPLOYMENT_STATUS}" -lt "${DEPLOYMENT_STATUS_SUCCEEDED}" ]]; then
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
  GCLOUD_USER=$(gcloud config get-value account)
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

      local SUCCESS_MESSAGE_TS
      SUCCESS_MESSAGE_TS=$(deployment_bot_message "${PROJECT_ID}" "${SLACK_CHANNEL_DEPLOYMENT_BOT}" "${DEPLOY_SUCCEEDED_MESSAGE}")
      upload_deployment_log "${PROJECT_ID}" "${COMMIT_HASH}" "${RELEASE_VERSION_TAG}" "${SUCCESS_MESSAGE_TS}"
      upload_internet_speed "${PROJECT_ID}" "${RELEASE_VERSION_TAG}" "${SUCCESS_MESSAGE_TS}"
      
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

    read -r -a VERSION_PARTS < <(parse_version "${GIT_VERSION_TAG}")
    MAJOR_VERSION="${VERSION_PARTS[1]}"
    MINOR_VERSION="${VERSION_PARTS[2]}"
    CHANGES_SINCE_TAG=$(git -C "$LOCAL_REPO_PATH" log "tags/${GIT_VERSION_TAG}..origin/releases/v${MAJOR_VERSION}.${MINOR_VERSION}-rc" --oneline) || exit_on_fail
    if [ -n "${CHANGES_SINCE_TAG}" ]; then
        echo "There are newly-added commits in the release branch that will not be deployed in ${GIT_VERSION_TAG}:"
        echo "${CHANGES_SINCE_TAG}" | indent_output
        echo "Folks may be expecting the above changes to go out in this production deploy (did we miss a cherry-pick?)."
        script_prompt  "Would you like to continue deploying ${GIT_VERSION_TAG}?"
    fi
}
