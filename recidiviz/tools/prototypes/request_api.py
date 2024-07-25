#!/usr/bin/env python

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
"""
This script should be run only after `docker-compose up` has been run, and the local app
is running in the background.

Make sure to run the case triage initialize development script to get local secrets.
./recidiviz/tools/prototypes/initialize_development_environment.sh

Example usage:

python -m recidiviz.tools.prototypes.request_api search '{"query":"housing updates"}' get --target_env dev

python -m recidiviz.tools.prototypes.request_api search '{"query":"housing updates"}' get --target_env staging --token {}

Optional params:
    --token: The user can provide a request token which would override the M2M token. A
        token is necessary for hitting the staging endpoint.

"""
import argparse
import json
from typing import Dict, Optional

import requests

from recidiviz.utils.secrets import get_secret_from_local_directory

LOCALHOST_URL = "http://localhost:8080/"
STAGING_URL = "https://prototypes-qqec6jbn6a-uc.a.run.app/"

request_string_to_request_type = {
    "get": "GET",
    "post": "POST",
    "delete": "DELETE",
}


def make_request_to_api(
    endpoint: str,
    body: Dict[str, str],
    request_type_str: str,
    target_env: str,
    token: Optional[str] = None,
) -> None:
    """Make a request to the case_note_search endpoint."""
    request_type = request_string_to_request_type.get(request_type_str)

    if not request_type:
        raise ValueError(
            "Register the {endpoint} method in `endpoint_to_request_type`."
        )

    s = requests.session()

    # If a token is not provided, use the M2M one.
    if token is None:
        auth0_dict = json.loads(
            get_secret_from_local_directory("dashboard_staging_auth0_m2m")
        )
        domain = auth0_dict["domain"]
        audience = auth0_dict["audience"]
        client_id = auth0_dict["clientId"]
        client_secret = get_secret_from_local_directory(
            "dashboard_staging_auth0_m2m_client_secret"
        )

        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "audience": audience,
            "grant_type": "client_credentials",
        }

        response = requests.post(f"https://{domain}/oauth/token", data=data, timeout=10)
        token = response.json()["access_token"]

    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

    # Make the request
    url = STAGING_URL if target_env == "staging" else LOCALHOST_URL
    try:
        if request_type == "GET":
            response = s.get(url + endpoint, headers=headers, params=body)
        elif request_type == "POST":
            response = s.post(url + endpoint, headers=headers, data=json.dumps(body))
        elif request_type == "DELETE":
            response = s.delete(url + endpoint, headers=headers)
        else:
            raise ValueError(f"Unsupported HttpMethod: {request_type}")

        response.raise_for_status()  # Raise an error for bad status codes
    except requests.exceptions.RequestException as e:
        print(f"Failed to connect to {url + endpoint}: {e}")
        print("Response content:", response.text)
    except json.JSONDecodeError:
        print("Failed to decode JSON response.")
        print("Response content:", response.text)

    print(response.status_code)
    print(json.dumps(response.json(), indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("endpoint", help="The name of the endpoint, e.g. search")
    parser.add_argument(
        "body",
        help="A JSON formatted string of params or data "
        + 'to pass to the endpoint, e.g. \'{"query": "job status"}\'',
    )
    parser.add_argument(
        "request_type_str", help="The name of the request type, e.g get"
    )
    parser.add_argument(
        "--target_env",
        help="What environment to point the request to",
        default="dev",
        choices=["dev", "staging"],
    )
    parser.add_argument("--token", help="Valid auth0 token to use")
    args = parser.parse_args()
    make_request_to_api(
        endpoint=args.endpoint,
        body=json.loads(args.body),
        request_type_str=args.request_type_str,
        target_env=args.target_env,
        token=args.token,
    )
