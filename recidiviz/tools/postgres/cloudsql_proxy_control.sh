#!/usr/bin/env bash
# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh
source ${BASH_SOURCE_DIR}/script_helpers.sh

CLOUD_SQL_PROXY_IMAGE="gcr.io/cloudsql-docker/gce-proxy:1.31.0"

DATABASE_CONNECTION_STRING=''
DATABASE_PORT=''
DRY_RUN=false
CONTROL=start

function print_usage {
    echo_error "usage: $0 -p LOCAL_PORT [-dq] [-c CONNECTION_STRING]"
    echo_error "  -p: (required) Port to expose the local database server on."
    echo_error "  -c: Connection string of the Cloud SQL instance. format: PROJECT_ID:REGION:INSTANCE_NAME"
    echo_error "      The connection string can be retrieved using gcloud secrets versions access latest --project=PROJECT_ID --secret=SCHEMA_TYPE_cloudsql_instance_id"
    echo_error "  -d: Dry run to check that the proxy can be started."
    echo_error "  -q: Quit any running Cloud SQL Proxy containers"
    run_cmd exit 1
}

function get_container_name {
  PORT=$1
  docker ps --format '{{.Names}}' --filter ancestor=${CLOUD_SQL_PROXY_IMAGE} --filter expose=${PORT}
}

while getopts "dqc:p:" flag; do
  case "${flag}" in
    d) DRY_RUN=true ;;
    c) DATABASE_CONNECTION_STRING="$OPTARG" ;;
    p) DATABASE_PORT="$OPTARG" ;;
    q) CONTROL=QUIT ;;
    *) print_usage
  esac
done

if [ "$DATABASE_PORT" == "" ]; then
  echo_error "Missing required argument -p"
  print_usage
fi

if [ $CONTROL == "QUIT" ]; then
  DOCKER_CONTAINER_ID=$(get_container_name $DATABASE_PORT)

  if [ "$DOCKER_CONTAINER_ID" == "" ]; then
    echo "No Cloud SQL Proxy containers are running on port ${DATABASE_PORT}."
  else
    echo "Killing Cloud SQL Proxy container on :${DATABASE_PORT} with name: ${DOCKER_CONTAINER_ID}"
    docker kill ${DOCKER_CONTAINER_ID} > /dev/null 2>&1
  fi

  exit 0
fi



if [ "$DATABASE_CONNECTION_STRING" == "" ]; then
  print_usage
fi


if [ $DRY_RUN == "true" ]; then
  if nc -z $CLOUDSQL_PROXY_HOST $DATABASE_PORT; then
    echo "The following processes are using port :${DATABASE_PORT}. Please quit the process bound to the port and try again."
    lsof -i :${DATABASE_PORT}

    echo "Run the following command to kill the docker containers (if applicable):"
    echo "$0 -q"
    exit 1
  fi

  exit 0
fi

echo "Starting Cloud SQL Proxy container for ${DATABASE_CONNECTION_STRING}..."


# The docker container runs with ~/.config/gcloud mounted as a volume to /config
# It assumes the existence of application_default_credentials.json
# Run `gcloud auth application-default login` if you are running into authentication errors
docker run -d \
  -p 127.0.0.1:$DATABASE_PORT:$DATABASE_PORT \
  -v ~/.config/gcloud:/config \
  $CLOUD_SQL_PROXY_IMAGE /cloud_sql_proxy \
  -instances=${DATABASE_CONNECTION_STRING}=tcp:0.0.0.0:$DATABASE_PORT \
  -credential_file=/config/application_default_credentials.json > /dev/null 2>&1

CONTAINER_NAME=$(get_container_name $DATABASE_PORT)
if [ $CONTAINER_NAME == "" ]; then
  echo "Failed to start the Cloud SQL Proxy. Check the docker logs for the most recently started container"
else
  echo "Started Cloud SQL Proxy container with name: ${CONTAINER_NAME}"
fi
