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

# Returns the last deployed version tag in a given project
function last_deployed_version_tag {
    PROJECT_ID=$1
    # Get tag and strip surrounding quotes.
    UNNORMALIZED_TAG=$(gcloud app versions list --project=$PROJECT_ID --hide-no-traffic --service=default --format=yaml | pipenv run yq .id | tr -d \") || exit_on_fail
    # Deployed version tags have all periods replaced with dashes, re-format to match git version tag format.
    LAST_DEPLOYED_GIT_VERSION_TAG=$(echo $UNNORMALIZED_TAG | tr '-' '.' | sed 's/.alpha/-alpha/g') || exit_on_fail

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

    MIGRATION_CHANGES=$(git diff tags/${LAST_VERSION_TAG} -- ${BASH_SOURCE_DIR}/../../../recidiviz/persistence/database/migrations) || exit_on_fail

    if [[ ! -z $MIGRATION_CHANGES ]]; then
      MIGRATION_CHANGES_SINCE_LAST_DEPLOY=1
    else
      MIGRATION_CHANGES_SINCE_LAST_DEPLOY=0
    fi

    echo $MIGRATION_CHANGES_SINCE_LAST_DEPLOY
}

# If there have been changes since the last deploy that indicate pipeline results may
# have changed, returns 1. Otherwise, if there have been no changes impacting
# pipelines, returns 0.
function calculation_pipeline_changes_since_last_deploy {
    PROJECT=$1

    if [[ ${PROJECT} == 'recidiviz-staging' ]]; then
        LAST_VERSION_TAG=$(last_version_tag_on_branch HEAD) || exit_on_fail
    elif [[ ${PROJECT} == 'recidiviz-123' ]]; then
        LAST_VERSION_TAG=$(last_deployed_version_tag recidiviz-123) || exit_on_fail
    else
        echo_error "Unexpected project for last version ${PROJECT}"
        exit 1
    fi

    MIGRATION_CHANGES_SINCE_LAST_DEPLOY=$(migration_changes_since_last_deploy $PROJECT)

    if [[ MIGRATION_CHANGES_SINCE_LAST_DEPLOY -eq 1 ]]; then
        echo $MIGRATION_CHANGES_SINCE_LAST_DEPLOY
    else
        CALCULATOR_CHANGES=$(git diff tags/${LAST_VERSION_TAG} -- ${BASH_SOURCE_DIR}/../../../recidiviz/calculator) || exit_on_fail

        if [[ ! -z $CALCULATOR_CHANGES ]]; then
          CALC_CHANGES_SINCE_LAST_DEPLOY=1
        else
          CALC_CHANGES_SINCE_LAST_DEPLOY=0
        fi

        echo $CALC_CHANGES_SINCE_LAST_DEPLOY
    fi
}

# Helper for deploying any infrastructure changes before we deploy a new version of the application. Requires that we
# have checked out the commit for the version that will be deployed.
function pre_deploy_configure_infrastructure {
    PROJECT=$1
    DOCKER_IMAGE_TAG=$2
    COMMIT_HASH=$3

    echo "Deploying terraform"
    verify_hash $COMMIT_HASH
    # Terraform determines certain resources by looking at the directory structure,
    # so give our shell the ability to open plenty of file descriptors.
    ulimit -n 1024 || exit_on_fail
    deploy_terraform_infrastructure ${PROJECT} ${COMMIT_HASH} ${DOCKER_IMAGE_TAG} || exit_on_fail

    echo "Running migrations on prod-data-client. You may have to enter the passphrase for your ssh key to continue."
    # The remote migration execution script doesn't play nice with run_cmd
    gcloud compute ssh --ssh-flag="-t" prod-data-client --command "cd pulse-data \
        && git fetch --all --tags --prune --prune-tags \
        && git checkout $COMMIT_HASH \
        && pipenv run ./recidiviz/tools/migrations/run_all_migrations.sh $COMMIT_HASH $PROJECT"
    exit_on_fail

    echo "Deploying cron.yaml"
    verify_hash $COMMIT_HASH
    run_cmd gcloud -q app deploy cron.yaml --project=${PROJECT}

    MIGRATION_CHANGES_SINCE_LAST_DEPLOY=$(migration_changes_since_last_deploy $PROJECT)

    if [[ MIGRATION_CHANGES_SINCE_LAST_DEPLOY -eq 1 ]]; then
        # Update the table schemas in BigQuery
        echo "Updating the BigQuery table schemas to match the versions being deployed"
        verify_hash $COMMIT_HASH
        run_cmd pipenv run python -m recidiviz.persistence.database.bq_refresh.big_query_table_manager --project_id ${PROJECT}
    fi

    # Update the Dataflow output table schemas and update all BigQuery views.
    echo "Updating the BigQuery Dataflow output table schemas to match the corresponding classes"
    verify_hash $COMMIT_HASH
    run_cmd pipenv run python -m recidiviz.calculator.dataflow_output_table_manager --project_id ${PROJECT}

    echo "Updating all BigQuery views"
    verify_hash $COMMIT_HASH
    run_cmd pipenv run python -m recidiviz.tools.deploy.deploy_views --project-id ${PROJECT}

    echo "Deploying calculation pipelines to templates in ${PROJECT}."
    deploy_pipeline_templates ${PROJECT} || exit_on_fail
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

    # Check that we're on at least the minimum version of Terraform
    TERRAFORM_VERSION=$(terraform --version | grep "^Terraform v" | cut -d ' ' -f 2 | sed 's/v//')
    MIN_REQUIRED_TERRAFORM_VERSION="1.1.9"

    if version_less_than ${TERRAFORM_VERSION} ${MIN_REQUIRED_TERRAFORM_VERSION}; then
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
    SERVING_VERSIONS=$(gcloud app versions list --project=${PROJECT_ID} --filter="SERVING_STATUS=SERVING" --format=yaml | pipenv run yq .id | wc -l | xargs) || exit_on_fail

    # Note: if we adjust the number of serving versions upward, we may
    # have to adjust the number of max connections in our postgres instances.
    # See the discussion in #5497 for more context, and see the docs:
    # https://cloud.google.com/sql/docs/quotas#postgresql for more.
    # See discussion in #6698 for additional rationale for bumping from 4 -> 8.



    # Each of the serving versions is 1 backend service. We also have a backend
    # service (most likely the load balancer for Case Triage that was added in #8829)
    # that is always always running, but isn't listed as one of the serving app
    # versions. We have a backend services quota of 9, and we need to be at least 2
    # under the quota in order to successfully complete a deploy of both the default
    # and scrapers services. So, if there are 7 or more serving versions then we need
    # to stop at least one to proceed.
    MAX_ALLOWED_SERVING_VERSIONS=6
    if [[ "$SERVING_VERSIONS" -gt "$MAX_ALLOWED_SERVING_VERSIONS" ]]; then
        echo_error "Found [$SERVING_VERSIONS] already serving versions. You must stop at least one version to proceed"
        echo_error "in order to avoid maxing out the number of allowed database connections."
        echo_error "Stop versions here: https://console.cloud.google.com/appengine/versions?organizationId=448885369991&project=$PROJECT_ID&serviceId=default"
        echo_error "          and here: https://console.cloud.google.com/appengine/versions?organizationId=448885369991&project=$PROJECT_ID&serviceId=scrapers"
        exit 1
    fi
    echo "Found [$SERVING_VERSIONS] already serving versions - proceeding"
}

function check_for_too_many_deployed_versions {
    PROJECT_ID=$1

    # Query for the deployed versions in YAML format, select the IDs, count the number of lines and trim whitespace
    DEPLOYED_VERSIONS=$(gcloud app versions list --project=${PROJECT_ID} --format=yaml | pipenv run yq .id | wc -l | xargs) || exit_on_fail
    # Our actual limit is 210 versions, but we safeguard against other versions being deployed before this deploy succeeds
    MAX_ALLOWED_DEPLOYED_VERSIONS=200
    if [[ "$DEPLOYED_VERSIONS" -ge "$MAX_ALLOWED_DEPLOYED_VERSIONS" ]]; then
        echo_error "Found [$DEPLOYED_VERSIONS] already deployed versions. You must delete at least one version to proceed"
        echo_error "in order to avoid maxing out the number of allowed deployed versions."
        echo_error "Delete versions here: https://console.cloud.google.com/appengine/versions?organizationId=448885369991&project=$PROJECT_ID&serviceId=default"
        echo_error "            and here: https://console.cloud.google.com/appengine/versions?organizationId=448885369991&project=$PROJECT_ID&serviceId=scrapers"
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
  rm -rf ${BASH_SOURCE_DIR}/.terraform/
  run_cmd terraform -chdir=${BASH_SOURCE_DIR}/terraform init -backend-config "bucket=${PROJECT_ID}-tf-state" -reconfigure
}

function deploy_pipeline_templates {
    PROJECT_ID=$1
    while true
    do
        verify_hash $COMMIT_HASH
        pipenv run python -m recidiviz.tools.deploy.deploy_all_pipeline_templates --project_id ${PROJECT_ID}
        return_code=$?

        if [[ $return_code -eq 0 ]]; then
            break
        fi
        script_prompt $'\n\nThere was an error deploying the pipeline templates. Check the logs in recidiviz/tools/deploy/log/deploy_all_pipeline_templates.log.\nWould you like to retry? [no exits the script]'
    done
}


function deploy_terraform_infrastructure {
    PROJECT_ID=$1
    GIT_HASH=$2
    DOCKER_IMAGE_TAG=$3

    echo "Starting terraform deployment..."

    while true
    do
        reconfigure_terraform_backend $PROJECT_ID
        run_cmd terraform -chdir=${BASH_SOURCE_DIR}/terraform plan -var="project_id=${PROJECT_ID}" -var="git_hash=${GIT_HASH}" -var="docker_image_tag=${DOCKER_IMAGE_TAG}" -out=tfplan
        script_prompt "Does the generated terraform plan look correct? [You can inspect it with \`terraform show tfplan\`]"

        CURRENT_TIME=$(date +'%s')
        PLAN_FILE_NAME=${DOCKER_IMAGE_TAG}-${GIT_HASH}-${CURRENT_TIME}.tfplan
        PLAN_FILE_PATH=gs://${PROJECT_ID}-tf-state/tf-plans/${PLAN_FILE_NAME}
        echo "Storing plan to ${PLAN_FILE_PATH} for posterity..."
        run_cmd terraform -chdir=${BASH_SOURCE_DIR}/terraform show tfplan > ${BASH_SOURCE_DIR}/terraform/tfplan.json
        run_cmd gsutil cp ${BASH_SOURCE_DIR}/terraform/tfplan.json $PLAN_FILE_PATH

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

function post_deploy_triggers {
    PROJECT=$1
    CALC_CHANGES_SINCE_LAST_DEPLOY=$2

    echo "Triggering post-deploy tasks"

    pipenv run python -m recidiviz.tools.deploy.trigger_post_deploy_tasks --project-id ${PROJECT} --trigger-historical-dag ${CALC_CHANGES_SINCE_LAST_DEPLOY}
}
