#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"
# shellcheck source=recidiviz/tools/postgres/script_helpers.sh
source "${BASH_SOURCE_DIR}/../postgres/script_helpers.sh"

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
function last_version_tag_on_branch {
    BRANCH=$1

    LAST_VERSION_TAG_ON_BRANCH=$(git tag --merged "${BRANCH}" | sort_versions | tail -n 1) || exit_on_fail

    # Check that the version parses
    _=$(parse_version "${LAST_VERSION_TAG_ON_BRANCH}") || exit_on_fail

    echo "${LAST_VERSION_TAG_ON_BRANCH}"
}

# Returns the last deployed version tag in a given project
function last_deployed_version_tag {
    PROJECT_ID=$1
    # Get tag and strip surrounding quotes.
    UNNORMALIZED_TAG=$(gcloud app versions list --project="$PROJECT_ID" --hide-no-traffic --service=default --format=yaml | pipenv run yq .id | tr -d \") || exit_on_fail
    # Deployed version tags have all periods replaced with dashes, re-format to match git version tag format.
    LAST_DEPLOYED_GIT_VERSION_TAG=$(echo "$UNNORMALIZED_TAG" | tr '-' '.' | sed 's/.alpha/-alpha/g') || exit_on_fail

    echo "${LAST_DEPLOYED_GIT_VERSION_TAG}"
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

# If there have been migrations since the last deploy, returns 1.
# Otherwise, if there have been no migrations, returns 0.
function migration_changes_since_last_deploy {
    PROJECT=$1

    if [[ ${PROJECT} == 'recidiviz-staging' ]]; then
        LAST_VERSION_TAG=$(last_version_tag_on_branch HEAD) || exit_on_fail
    elif [[ ${PROJECT} == 'recidiviz-123' ]]; then
        LAST_VERSION_TAG=$(last_deployed_version_tag recidiviz-123) || exit_on_fail
    else
        echo_error "Unexpected project for last version ${PROJECT}"
        exit 1
    fi

    MIGRATION_CHANGES=$(git diff "tags/${LAST_VERSION_TAG}" -- "${BASH_SOURCE_DIR}/../../../recidiviz/persistence/database/migrations") || exit_on_fail

    if [[ -n $MIGRATION_CHANGES ]]; then
      MIGRATION_CHANGES_SINCE_LAST_DEPLOY=1
    else
      MIGRATION_CHANGES_SINCE_LAST_DEPLOY=0
    fi

    echo $MIGRATION_CHANGES_SINCE_LAST_DEPLOY
}

# Helper for deploying any infrastructure changes before we deploy a new version of the application. Requires that we
# have checked out the commit for the version that will be deployed.
function pre_deploy_configure_infrastructure {
    PROJECT=$1
    DOCKER_IMAGE_TAG=$2
    COMMIT_HASH=$3

    echo "Deploying terraform"
    verify_hash "$COMMIT_HASH"
    # Terraform determines certain resources by looking at the directory structure,
    # so give our shell the ability to open plenty of file descriptors.
    ulimit -n 1024 || exit_on_fail
    deploy_terraform_infrastructure "${PROJECT}" "${COMMIT_HASH}" "${DOCKER_IMAGE_TAG}" || exit_on_fail

    deploy_migrations "${PROJECT}" "${COMMIT_HASH}"
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
    MIN_REQUIRED_TERRAFORM_VERSION="1.7.0"

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

function check_for_too_many_serving_versions {
    PROJECT_ID=$1

    # Query for the serving versions in YAML format, select the IDs, count the number of lines and trim whitespace
    SERVING_VERSIONS=$(gcloud app versions list --project="${PROJECT_ID}" --filter="SERVING_STATUS=SERVING" --format=yaml | pipenv run yq .id | wc -l | xargs) || exit_on_fail

    # Note: if we adjust the number of serving versions upward, we may
    # have to adjust the number of max connections in our postgres instances.
    # See the discussion in #5497 for more context, and see the docs:
    # https://cloud.google.com/sql/docs/quotas#postgresql for more.
    # See discussion in #6698 for additional rationale for bumping from 4 -> 8.



    # Each of the serving versions is 1 backend service. We also have a backend
    # service (most likely the load balancer for Case Triage that was added in #8829)
    # that is always always running, but isn't listed as one of the serving app
    # versions. We have a backend services quota of 9, and we need to be at least 1
    # under the quota in order to successfully complete a deploy of the default service.
    # So, if there are 8 or more serving versions then we need to stop at least one to
    # proceed.
    MAX_ALLOWED_SERVING_VERSIONS=7
    if [[ "$SERVING_VERSIONS" -gt "$MAX_ALLOWED_SERVING_VERSIONS" ]]; then
        echo_error "Found [$SERVING_VERSIONS] already serving versions. You must stop at least one version to proceed"
        echo_error "in order to avoid maxing out the number of allowed database connections."
        echo_error "Stop versions here: https://console.cloud.google.com/appengine/versions?organizationId=448885369991&project=$PROJECT_ID&serviceId=default"
        exit 1
    fi
    echo "Found [$SERVING_VERSIONS] already serving versions - proceeding"
}

function check_for_too_many_deployed_versions {
    PROJECT_ID=$1

    # Query for the deployed versions in YAML format, select the IDs, count the number of lines and trim whitespace
    DEPLOYED_VERSIONS=$(gcloud app versions list --project="${PROJECT_ID}" --format=yaml | pipenv run yq .id | wc -l | xargs) || exit_on_fail
    # Our actual limit is 210 versions, but we safeguard against other versions being deployed before this deploy succeeds
    MAX_ALLOWED_DEPLOYED_VERSIONS=200
    if [[ "$DEPLOYED_VERSIONS" -ge "$MAX_ALLOWED_DEPLOYED_VERSIONS" ]]; then
        echo_error "Found [$DEPLOYED_VERSIONS] already deployed versions. You must delete at least one version to proceed"
        echo_error "in order to avoid maxing out the number of allowed deployed versions."
        echo_error "Delete versions here: https://console.cloud.google.com/appengine/versions?organizationId=448885369991&project=$PROJECT_ID&serviceId=default"
        exit 1
    fi
    echo "Found [$DEPLOYED_VERSIONS] already deployed versions - proceeding"
}

function verify_can_deploy {
    PROJECT_ID=$1

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

    echo "Checking for too many deployed versions"
    run_cmd check_for_too_many_deployed_versions "${PROJECT_ID}"

    echo "Checking for too many currently serving versions"
    run_cmd check_for_too_many_serving_versions "${PROJECT_ID}"

    echo "Checking pipenv is synced"
    "${BASH_SOURCE_DIR}"/../diff_pipenv.sh || exit_on_fail
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


function deploy_terraform_infrastructure {
    PROJECT_ID=$1
    GIT_HASH=$2
    DOCKER_IMAGE_TAG=$3
    TF_STATE_PREFIX=""
    PAGERDUTY_TOKEN=$(get_secret "$PROJECT_ID" pagerduty_terraform_key) || exit_on_fail

    echo "Starting terraform deployment..."

    while true
    do
        reconfigure_terraform_backend "$PROJECT_ID" "$TF_STATE_PREFIX"
        run_cmd terraform -chdir="${BASH_SOURCE_DIR}/terraform" plan -var="project_id=${PROJECT_ID}" -var="git_hash=${GIT_HASH}" -var="pagerduty_token=${PAGERDUTY_TOKEN}" -var="docker_image_tag=${DOCKER_IMAGE_TAG}" -out=tfplan
        script_prompt "Does the generated terraform plan look correct? [You can inspect it with \`terraform show tfplan\`]"

        CURRENT_TIME=$(date +'%s')
        PLAN_FILE_NAME=${DOCKER_IMAGE_TAG}-${GIT_HASH}-${CURRENT_TIME}.tfplan
        PLAN_FILE_PATH=gs://${PROJECT_ID}-tf-state/tf-plans/${PLAN_FILE_NAME}
        echo "Storing plan to ${PLAN_FILE_PATH} for posterity..."
        run_cmd terraform -chdir="${BASH_SOURCE_DIR}/terraform" show tfplan > "${BASH_SOURCE_DIR}/terraform/tfplan.json"
        run_cmd gsutil cp "${BASH_SOURCE_DIR}/terraform/tfplan.json" "$PLAN_FILE_PATH"

        echo "Applying the terraform plan..."
        # not using run_cmd because we don't want to exit_on_fail
        terraform -chdir=./recidiviz/tools/deploy/terraform apply tfplan | indent_output
        return_code=$?
        rm ./recidiviz/tools/deploy/terraform/tfplan
        rm ./recidiviz/tools/deploy/terraform/tfplan.json

        if [[ $return_code -eq 0 ]]; then
            break
        fi
        script_prompt "There was an error applying the terraform plan. Would you like to re-plan and retry? [no exits the script]"
    done

    echo "Terraform deployment complete."
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

function deploy_migrations {
  local PROJECT=$1
  local COMMIT_HASH=$2

  while true
  do
    echo "Running migrations using Cloud SQL Proxy"

    run_cmd_no_exiting pipenv run ./recidiviz/tools/migrations/run_all_migrations.sh "${COMMIT_HASH}" "${PROJECT}"
    RETURN_CODE=$?

    if [[ $RETURN_CODE -eq 0 ]]; then
      echo "Successfully ran migrations"
      return 0
    fi

    # Migrations did not run successfully. Check if there was an error in the Cloud SQL Proxy
    run_cmd_no_exiting pipenv run ./recidiviz/tools/postgres/cloudsql_proxy_control.sh -v -p "${CLOUDSQL_PROXY_MIGRATION_PORT}"
    RETURN_CODE=$?

    if [[ $RETURN_CODE -eq $CLOUDSQL_PROXY_NETWORK_ERROR_EXIT_CODE ]]; then
      script_prompt "There was an intermittent network error while applying migrations. Would you like to retry?"
    else
      echo "There was an error running migrations, due to incorrect application logic or an intermittent network error."
      echo "Please review the source of the error."
      script_prompt "Would you like to retry applying migrations?"
    fi
  done
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


  if [[ "${DEPLOYMENT_STATUS}" < "${DEPLOYMENT_STATUS_SUCCEEDED}" ]]; then
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
