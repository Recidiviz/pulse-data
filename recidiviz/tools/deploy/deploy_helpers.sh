#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

VERSION_REGEX="^v([0-9]+)\.([0-9]+)\.([0-9]+)(-alpha.([0-9]+))?$"

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

    echo ${BASH_REMATCH[@]}
}

# Returns the last version tag on the given branch. Fails if that tag does not match the acceptable version regex.
function last_version_tag_on_branch {
    BRANCH=$1

    LAST_VERSION_TAG_ON_BRANCH=$(git tag --merged ${BRANCH} | sort_versions | tail -n 1) || exit_on_fail

    # Check that the version parses
    _=$(parse_version ${LAST_VERSION_TAG_ON_BRANCH}) || exit_on_fail

    echo ${LAST_VERSION_TAG_ON_BRANCH}
}

# Returns the last deployed production version tag
function last_deployed_production_version_tag {
    LAST_DEPLOYED_GIT_VERSION_TAG=$(gcloud app versions list --project=recidiviz-123 --hide-no-traffic --service=default --format=yaml | pipenv run yq .id | tr -d \" | tr '-' '.') || exit_on_fail

    echo ${LAST_DEPLOYED_GIT_VERSION_TAG}
}

function next_alpha_version {
    PREVIOUS_VERSION=$1
    PREVIOUS_VERSION_PARTS=($(parse_version ${PREVIOUS_VERSION})) || exit_on_fail

    MAJOR=${PREVIOUS_VERSION_PARTS[1]}
    MINOR=${PREVIOUS_VERSION_PARTS[2]}
    PATCH=${PREVIOUS_VERSION_PARTS[3]}
    ALPHA=${PREVIOUS_VERSION_PARTS[4]-}  # Optional
    ALPHA_VERSION=${PREVIOUS_VERSION_PARTS[5]-}  # Optional

    if [[ -z ${ALPHA} ]]; then
        # If the previous version was a release version, bump the minor version and build a fresh alpha version
        NEW_VERSION="v$MAJOR.$(($MINOR + 1)).0-alpha.0"
    else
        # If the previous version was an alpha version, just increment alpha version
        NEW_VERSION="v$MAJOR.$MINOR.$PATCH-alpha.$(($ALPHA_VERSION + 1))"
    fi

    echo ${NEW_VERSION}
}

# If there have been changes since the last deploy that indicate pipeline results may have changed, returns a non-empty
# string. Otherwise, if there have been no changes impacting pipelines, returns an empty result.
function calculation_pipeline_changes_since_last_deploy {
    PROJECT=$1

    if [[ ${PROJECT} == 'recidiviz-staging' ]]; then
        LAST_VERSION_TAG=$(last_version_tag_on_branch HEAD) || exit_on_fail
    elif [[ ${PROJECT} == 'recidiviz-123' ]]; then
        LAST_VERSION_TAG=$(last_deployed_production_version_tag) || exit_on_fail
    else
        echo_error "Unexpected project for last version ${PROJECT}"
        exit 1
    fi

    MIGRATION_CHANGES=$(git diff tags/${LAST_VERSION_TAG} -- ${BASH_SOURCE_DIR}/../../../recidiviz/persistence/database/migrations)
    CALCULATOR_CHANGES=$(git diff tags/${LAST_VERSION_TAG} -- ${BASH_SOURCE_DIR}/../../../recidiviz/calculator)

    echo "${MIGRATION_CHANGES}${CALCULATOR_CHANGES}"
}

# Helper for deploying any infrastructure changes before we deploy a new version of the application. Requires that we
# have checked out the commit for the version that will be deployed.
function pre_deploy_configure_infrastructure {
    PROJECT=$1
    DOCKER_IMAGE_TAG=$2
    DEBUG_BUILD_NAME=$3

    echo "Deploying cron.yaml"
    run_cmd gcloud -q app deploy cron.yaml --project=${PROJECT}

    echo "Initializing task queues"
    run_cmd pipenv run python -m recidiviz.tools.initialize_google_cloud_task_queues --project_id ${PROJECT} --google_auth_token $(gcloud auth print-access-token)

    if [[ -z ${DEBUG_BUILD_NAME} ]]; then
        # If it's not a debug build (i.e. local to staging), we update the Dataflow metric table schemas and update all BigQuery views.
        echo "Updating the BigQuery Dataflow metric table schemas to match the metric classes"
        run_cmd pipenv run python -m recidiviz.calculator.dataflow_metric_table_manager --project_id ${PROJECT}

        echo "Updating all BigQuery views"
        run_cmd pipenv run python -m recidiviz.big_query.view_update_manager --project_id ${PROJECT} --views_to_update all --materialized_views_only False
    else
        echo "Skipping BigQuery table and view updates for debug build."
    fi

    if [[ -z ${DEBUG_BUILD_NAME} ]]; then
        # If it's not a debug build (i.e. local to staging), we deploy pipeline templates.
        if [[ ${PROJECT} == 'recidiviz-staging' ]]; then
            echo "Deploying stage-only calculation pipelines to templates in ${PROJECT}."
            run_cmd pipenv run python -m recidiviz.tools.deploy.deploy_pipeline_templates --project_id ${PROJECT} --templates_to_deploy staging
        fi

        echo "Deploying prod-ready calculation pipelines to templates in ${PROJECT}."
        run_cmd pipenv run python -m recidiviz.tools.deploy.deploy_pipeline_templates --project_id ${PROJECT} --templates_to_deploy production
    else
        echo "Skipping pipeline template deploy for debug build."
    fi

    if [[ -z ${DEBUG_BUILD_NAME} ]]; then
        deploy_terraform_infrastructure ${PROJECT} $(git rev-parse HEAD) ${DOCKER_IMAGE_TAG} || exit_on_fail
    else
        echo "Skipping terraform changes for debug build."
    fi
}

function check_running_in_pipenv_shell {
    if [[ -z $(printenv PIPENV_ACTIVE) ]]; then
        echo_error "Must be running inside the pipenv shell to deploy."
        exit 1
    fi
}

function check_docker_running {
    if [[ -z $(which docker) ]]; then
        echo_error "Docker not installed. Please follow instructions in repo README to install."
        echo_error "Also make sure you've configured gcloud docker permissions with:"
        echo_error "    $ gcloud auth login"
        echo_error "    $ gcloud auth configure-docker"
        exit 1
    fi

    docker info > /dev/null 2>&1
    if [[ $? -ne 0 ]]; then
        echo_error "The docker daemon doesn't seem to be running. Please start it before continuing the script."
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

    # Check that we're on version 0.14.x
    terraform --version | grep "^Terraform v0.14." > /dev/null
    if [[ $? -ne 0 ]]; then
        echo_error "The installed version of \`terraform\` is not v0.14.x. Please reinstall v0.14 of Terraform."
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
    SERVING_VERSIONS=$(gcloud app versions list --project=${PROJECT_ID} --service=default --filter="SERVING_STATUS=SERVING" --format=yaml | pipenv run yq .id | wc -l | xargs) || exit_on_fail

    # Note: if we adjust the number of serving versions upward, we may
    # have to adjust the number of max connections in our postgres instances.
    # See the dicussion in #5497 for more context, and see the docs:
    # https://cloud.google.com/sql/docs/quotas#postgresql for more.
    MAX_ALLOWED_SERVING_VERSIONS=4
    if [[ "$SERVING_VERSIONS" -ge "$MAX_ALLOWED_SERVING_VERSIONS" ]]; then
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
    DEPLOYED_VERSIONS=$(gcloud app versions list --project=${PROJECT_ID} --service=default --format=yaml | pipenv run yq .id | wc -l | xargs) || exit_on_fail
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

    echo "Checking Docker is installed and running"
    run_cmd check_docker_running

    echo "Checking jq is installed"
    run_cmd check_jq_installed

    echo "Checking terraform is installed"
    run_cmd check_terraform_installed

    echo "Checking for too many deployed versions"
    run_cmd check_for_too_many_deployed_versions ${PROJECT_ID}

    echo "Checking for too many currently serving versions"
    run_cmd check_for_too_many_serving_versions ${PROJECT_ID}

    echo "Checking pipenv is synced"
    ${BASH_SOURCE_DIR}/../diff_pipenv.sh || exit_on_fail
}

function reconfigure_terraform_backend {
  PROJECT_ID=$1
  echo "Reconfiguring Terraform backend..."
  rm -rf .terraform/
  run_cmd terraform init -backend-config "bucket=${PROJECT_ID}-tf-state" ${BASH_SOURCE_DIR}/terraform
}


function deploy_terraform_infrastructure {
    PROJECT_ID=$1
    GIT_HASH=$2
    DOCKER_IMAGE_TAG=$3

    echo "Starting terraform deployment..."
    reconfigure_terraform_backend $PROJECT_ID

    while true
    do
        run_cmd terraform plan -var="project_id=${PROJECT_ID}" -var="git_hash=${GIT_HASH}" -var="docker_image_tag=${DOCKER_IMAGE_TAG}" -out=tfplan ${BASH_SOURCE_DIR}/terraform
        script_prompt "Does the generated terraform plan look correct? [You can inspect it with \`terraform show tfplan\`]"

        echo "Applying the terraform plan..."
        # not using run_cmd because we don't want to exit_on_fail
        terraform apply tfplan | indent_output
        return_code=$?
        rm ./tfplan

        if [[ $return_code -eq 0 ]]; then
            break
        fi
        script_prompt "There was an error applying the terraform plan. Would you like to re-plan and retry? [no exits the script]"
    done

    echo "Terraform deployment complete."
}

function post_deploy_triggers {
    PROJECT=$1

    if [[ ! -z $(calculation_pipeline_changes_since_last_deploy ${PROJECT}) ]]; then
        # We trigger historical calculations with every deploy where we believe there could be code changes that impact
        # historical metric output.
        echo "Triggering historical calculation pipelines"

        # Note: using exit_on_fail instead of run_cmd since the quoted string doesn't translate well when passed to run_cmd
        gcloud pubsub topics publish v1.calculator.historical_incarceration_us_nd --project ${PROJECT} --message="Trigger Dataflow job" || exit_on_fail
        gcloud pubsub topics publish v1.calculator.historical_supervision_us_nd --project ${PROJECT} --message="Trigger Dataflow job" || exit_on_fail
    else
        echo "Skipping historical calculation pipeline trigger - no relevant code changes"
    fi
}
