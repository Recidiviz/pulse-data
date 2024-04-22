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

export CLOUDSQL_PROXY_HOST=127.0.0.1
export CLOUDSQL_PROXY_PORT=5439
export CLOUDSQL_PROXY_MIGRATION_PORT=5440

# Exit codes
export CLOUDSQL_PROXY_NETWORK_ERROR_EXIT_CODE=2
export CLOUDSQL_PROXY_NEVER_STARTED_ERROR_EXIT_CODE=3

export CLOUDSQL_PROXY_IMAGE_NAME="gcr.io/cloudsql-docker/gce-proxy"
export CLOUDSQL_PROXY_IMAGE_VERSION="1.31.0"
export CLOUDSQL_PROXY_IMAGE="${CLOUDSQL_PROXY_IMAGE_NAME}:${CLOUDSQL_PROXY_IMAGE_VERSION}"

function vpn_is_installed () {
  open -Ra "Jamf Trust"
}

function disable_vpn () {
  vpnStatus=$(scutil --nc list | grep Connected)
  if [[ "$vpnStatus" = *VPN:com.jamf.trust* ]]; then
    open -a "Jamf Trust" "com.jamf.trust://?action=disable_vpn"
    sleep 5
  fi
  while [[ "$vpnStatus" = *VPN:com.jamf.trust* ]]
  do
    echo "Waiting for VPN to disconnect"
    sleep 2
    vpnStatus=$(scutil --nc list | grep Connected)
  done
}

function enable_vpn () {
  vpnStatus=$(scutil --nc list | grep Connected)
  if [[ "$vpnStatus" != *VPN:com.jamf.trust* ]]; then
    open -a "Jamf Trust" "com.jamf.trust://?action=enable_vpn"
    sleep 5
  fi
  while [[ "$vpnStatus" != *VPN:com.jamf.trust* ]]
  do
    echo "Waiting for VPN to reconnect"
    sleep 2
    vpnStatus=$(scutil --nc list | grep Connected)
  done
}

function set_vpn_status () {
  if vpn_is_installed ; then
    if [[ $1 = "Disable" ]]; then
      disable_vpn
    else
      enable_vpn
    fi
  else
     echo "VPN not installed on machine. Skipping this step."
  fi
}


function wait_for_postgres () {
  HOST=$1
  PORT=$2
  ATTEMPTS=0

  until pg_isready -q --host="${HOST}" --port="${PORT}"; do
    ATTEMPTS=$((ATTEMPTS + 1))

    if [[ $ATTEMPTS -eq 10 ]]; then
      echo "Postgres ${HOST}:${PORT}} was not ready in time"
      exit 1
    fi

    sleep 1;
  done
}
