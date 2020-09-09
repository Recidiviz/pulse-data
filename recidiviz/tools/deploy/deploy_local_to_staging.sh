#!/usr/bin/env bash

deploy_name=$1
version_tag=$(echo $2 | tr '.' '-')

if [[ x"$deploy_name" == x || x"$version_tag" == x ]]; then
    echo "usage: $0 <deploy_name> <version_tag>"
    exit 1
fi

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

# Deploys a debug version to staging without promoting traffic to it
${BASH_SOURCE_DIR}/base_deploy_to_staging.sh -v ${version_tag} -d ${deploy_name} -n
