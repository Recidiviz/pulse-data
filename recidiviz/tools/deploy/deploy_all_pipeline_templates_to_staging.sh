#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")

source ${BASH_SOURCE_DIR}/deploy_pipeline_helpers.sh

echo "Deploying all pipeline templates to staging"
deploy_pipeline_templates_to_staging
