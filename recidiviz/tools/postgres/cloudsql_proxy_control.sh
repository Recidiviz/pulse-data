#!/usr/bin/env bash
# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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


# Example usages:
# # Start the proxy
# ✗ ./recidiviz/tools/postgres/cloudsql_proxy_control.sh -p 5440 -c PROJECT_ID:GCP_REGION:INSTANCE_NAME
# Starting Cloud SQL Proxy container for PROJECT_ID:GCP_REGION:INSTANCE_NAME...
# Started Cloud SQL Proxy container with name: dazzling_mclean
#
# # See if the proxy can be started by performing a dry run
# ✗ ./recidiviz/tools/postgres/cloudsql_proxy_control.sh -p 5440 -c PROJECT_ID:GCP_REGION:INSTANCE_NAME -d
# Connection to 127.0.0.1 port 5440 [tcp/*] succeeded!
# The following processes are using port :5440. Please quit the process bound to the port and try again.
# COMMAND    PID      USER   FD   TYPE             DEVICE SIZE/OFF NODE NAME
# com.docke 4747 danhansen  137u  IPv4 0xe91a990d7862af27      0t0  TCP localhost:5440->localhost:49207 (CLOSE_WAIT)
# com.docke 4747 danhansen  139u  IPv4 0xe91a990d7bbc19c7      0t0  TCP localhost:5440 (LISTEN)
# Run the following command to kill the docker containers (if applicable):
# ./recidiviz/tools/postgres/cloudsql_proxy_control.sh -q -p 5440
#
# # Verify proxy is running
# ✗ ./recidiviz/tools/postgres/cloudsql_proxy_control.sh -p 5440 -c PROJECT_ID:GCP_REGION:INSTANCE_NAME -v
# The Cloud SQL Proxy is running and accepting connections.
#
# # Quit proxy
# ✗ ./recidiviz/tools/postgres/cloudsql_proxy_control.sh -p 5440 -c PROJECT_ID:GCP_REGION:INSTANCE_NAME -q
# Killing Cloud SQL Proxy container on :5440 with name: dazzling_mclean
#
# # Verify that the container executed without encountering network errors
# ✗ ./recidiviz/tools/postgres/cloudsql_proxy_control.sh -p 5440 -c PROJECT_ID:GCP_REGION:INSTANCE_NAME -v
# No known errors encountered in the Cloud SQL Proxy container.


BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"
# shellcheck source=recidiviz/tools/postgres/script_helpers.sh
source "${BASH_SOURCE_DIR}/script_helpers.sh"

function print_usage {
    echo_error "usage: $0 -p LOCAL_PORT [-dqv] [-c CONNECTION_STRING]"
    echo_error "  -p: (required) Port to expose the local database server on."
    echo_error "  -c: Connection string of the Cloud SQL instance. format: PROJECT_ID:REGION:INSTANCE_NAME"
    echo_error "      The connection string can be retrieved using gcloud secrets versions access latest --project=PROJECT_ID --secret=SCHEMA_TYPE_cloudsql_instance_id"
    echo_error "  -d: Dry run to check that the proxy can be started."
    echo_error "  -v: Verify that the container behaved as we expected. Exits with non-zero if not."
    echo_error "  -q: Quit any running Cloud SQL Proxy containers"
    run_cmd exit 1
}

function prune_old_containers {
  # Prunes CloudSQL Proxy containers from more than 1 week ago
  EXITED_CONTAINERS=$(get_exited_containers)
  if [[ $EXITED_CONTAINERS != "" ]]; then
    docker rm "${EXITED_CONTAINERS}" > /dev/null
  fi
}

function get_ancestor_filters {
  # Gets `docker ps` filter arguments for all versions of the Cloud SQL proxy image
  declare -a LOCAL_IMAGES
  read -r -d '\n' -a LOCAL_IMAGES < <(docker images --filter "reference=${CLOUDSQL_PROXY_IMAGE_NAME}:*" --format "{{.ID}}")

  ANCESTOR_FILTERS=()

  for i in "${!LOCAL_IMAGES[@]}"; do
    IMAGE_NAME="${LOCAL_IMAGES[$i]}"
    ANCESTOR_FILTERS[$i]="--filter ancestor=${IMAGE_NAME}"
  done

  echo "${ANCESTOR_FILTERS[@]}"
}

function get_container_name {
  PORT=$1
  declare -a ANCESTOR_FILTERS
  read -r -a ANCESTOR_FILTERS < <(get_ancestor_filters)
  docker ps --format '{{.Names}}' "${ANCESTOR_FILTERS[@]}" --filter expose="${PORT}"
}

function get_exited_containers {
  # Exited containers no longer have ports associated, so they don't match the `expose` filter in `get_container_name`
  # Duplicating the ps call here
  declare -a ANCESTOR_FILTERS
  read -r -a ANCESTOR_FILTERS < <(get_ancestor_filters)
  docker ps --format '{{.Names}}' "${ANCESTOR_FILTERS[@]}" --filter "status=exited"
}

function control_quit {
  # Quit running proxies on the specified port
  DOCKER_CONTAINER_ID=$(get_container_name "$DATABASE_PORT")

  if [ "$DOCKER_CONTAINER_ID" == "" ]; then
    echo "No Cloud SQL Proxy containers are running on port ${DATABASE_PORT}."
  else
    echo "Killing Cloud SQL Proxy container on :${DATABASE_PORT} with name: ${DOCKER_CONTAINER_ID}"
    docker kill "${DOCKER_CONTAINER_ID}" > /dev/null 2>&1
    echo "Cleaning up stopped Cloud SQL Proxy container with name: ${DOCKER_CONTAINER_ID}"
    docker rm "${DOCKER_CONTAINER_ID}" > /dev/null 2>&1
  fi

  exit 0
}

function control_verify {
  # Verify that the proxy is running, if it is not, double check the logs to verify that it ran successfully
  DOCKER_CONTAINER_ID=$(get_container_name "$DATABASE_PORT")

  if [ "$DOCKER_CONTAINER_ID" ]; then
    wait_for_postgres "$DATABASE_HOST" "$DATABASE_PORT"
    echo "The Cloud SQL Proxy is running and accepting connections."
    exit 0
  fi

  DOCKER_CONTAINER_ID=$(get_exited_containers | head -n 1)

  if [ -z "$DOCKER_CONTAINER_ID" ]; then
    echo "Could not find any existing proxy containers for port ${DATABASE_PORT}"
    exit "$CLOUDSQL_PROXY_NEVER_STARTED_ERROR_EXIT_CODE"
  fi

  CONTAINER_LOGS=$(docker logs "${DOCKER_CONTAINER_ID}" 2>&1)

  # Check for any client networks errors, such as losing connection, or connection being reset
  ENCOUNTERED_NETWORK_ERROR=$(echo -e "$CONTAINER_LOGS" | grep -ie "connection refused" -e "connection reset by peer")

  if [[ $ENCOUNTERED_NETWORK_ERROR != "" ]]; then
    echo "The Cloud SQL Proxy container encountered a network error:"
    echo "${ENCOUNTERED_NETWORK_ERROR}"
    exit "$CLOUDSQL_PROXY_NETWORK_ERROR_EXIT_CODE"
  else
    echo "No known errors encountered in the Cloud SQL Proxy container."
    echo "${CONTAINER_LOGS}"
  fi

  exit 0
}

function control_start {
   if [ "$DATABASE_CONNECTION_STRING" == "" ]; then
    print_usage
  fi


  if [ "$DRY_RUN" == "true" ]; then
    if nc -z "$DATABASE_HOST" "$DATABASE_PORT"; then
      echo "The following processes are using port :${DATABASE_PORT}. Please quit the process bound to the port and try again."
      lsof -i :"${DATABASE_PORT}"

      echo "Run the following command to kill the docker containers (if applicable):"
      echo "$0 -q -p ${DATABASE_PORT}"
      exit 1
    fi

    exit 0
  fi

  echo "Starting Cloud SQL Proxy container for ${DATABASE_CONNECTION_STRING}..."

  # The docker container runs with ~/.config/gcloud mounted as a volume to /config
  # It assumes the existence of application_default_credentials.json
  # Run `gcloud auth application-default login` if you are running into authentication errors
  docker run -d \
    --user 0 \
    -p "$DATABASE_HOST":"$DATABASE_PORT":"$DATABASE_PORT" \
    -v ~/.config/gcloud:/config \
    "$CLOUDSQL_PROXY_IMAGE" \
    "${DATABASE_CONNECTION_STRING}" \
    --address 0.0.0.0 \
    --port "${DATABASE_PORT}" \
    --credentials-file=/config/application_default_credentials.json > /dev/null 2>&1

  CONTAINER_NAME=$(get_container_name "$DATABASE_PORT")

  if [ "$CONTAINER_NAME" == "" ]; then
    echo "Failed to start the Cloud SQL Proxy. Check the docker logs for the most recently started container"
    exit "$CLOUDSQL_PROXY_NEVER_STARTED_ERROR_EXIT_CODE"
  else
    echo "Started Cloud SQL Proxy container with name: ${CONTAINER_NAME}"
    echo "If you encounter issues with connecting, check the logs \`docker logs ${CONTAINER_NAME}\`"
  fi

  # Wait for Postgres to be ready before continuing
  wait_for_postgres "$DATABASE_HOST" "$DATABASE_PORT"
}

# Script body
check_docker_running

DATABASE_CONNECTION_STRING=''
DATABASE_HOST=$CLOUDSQL_PROXY_HOST
DATABASE_PORT=''
DRY_RUN=false
CONTROL=START

while getopts "dqvc:p:" flag; do
  case "${flag}" in
    d) DRY_RUN=true ;;
    c) DATABASE_CONNECTION_STRING="$OPTARG" ;;
    p) DATABASE_PORT="$OPTARG" ;;
    q) CONTROL=QUIT ;;
    v) CONTROL=VERIFY ;;
    *) print_usage
  esac
done

if [ "$DATABASE_PORT" == "" ]; then
  echo_error "Missing required argument -p"
  print_usage
fi

case $CONTROL in
  QUIT) control_quit ;;
  VERIFY) control_verify ;;
  START) control_start ;;
  **) print_usage ;;
esac
