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
"""
Tool for making requests to the Justice Counts Control Panel API.

This script should be run only after `docker-compose up` has been run, and the local app
is running in the background. You should also run the `load_fixtures` script first so
the local database is populated with test objects.

Example usage:
python -m recidiviz.tools.justice_counts.control_panel.request_api reports '{"user_id":0,"agency_id": 0}'
python -m recidiviz.tools.justice_counts.control_panel.request_api users '{"email_address":"jsmith@gmail.com"}'
"""
import argparse
import json
from typing import Dict

import requests

from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import HttpMethod
from recidiviz.utils.secrets import get_secret_from_local_directory

LOCALHOST_URL = "http://localhost:5000/api/"


request_string_to_request_type = {
    "get": HttpMethod.GET,
    "post": HttpMethod.POST,
}


def make_request_to_api(
    endpoint: str, body: Dict[str, str], request_type_str: str
) -> None:
    """Make a request to the Justice Counts Control Panel API using Client Credentials authorization.
    More details: https://auth0.com/docs/get-started/authentication-and-authorization-flow/client-credentials-flow
    """
    request_type = request_string_to_request_type.get(request_type_str)

    if not request_type:
        raise ValueError(
            "Register the {endpoint} method in `endpoint_to_request_type`."
        )

    auth0_dict = json.loads(get_secret_from_local_directory("justice_counts_auth0"))
    domain = auth0_dict["domain"]
    audience = auth0_dict["audience"]
    client_id = get_secret_from_local_directory("justice_counts_m2m_client_id")
    client_secret = get_secret_from_local_directory("justice_counts_m2m_client_secret")
    # First obtain an access_token via a client_credientials grant
    # More details: https://auth0.com/docs/get-started/authentication-and-authorization-flow/client-credentials-flow
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": audience,
        "grant_type": "client_credentials",
    }

    response = requests.post(f"https://{domain}/oauth/token", data=data, timeout=10)
    token = response.json()["access_token"]

    s = requests.session()
    # Next get a valid CSRF token
    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    response = s.get(LOCALHOST_URL + "init", headers=headers)
    headers["X-CSRF-Token"] = response.json()["csrf"]

    # Finally, make the request
    if request_type == HttpMethod.GET:
        response = s.get(LOCALHOST_URL + endpoint, headers=headers, params=body)
    elif request_type == HttpMethod.POST:
        response = s.post(
            LOCALHOST_URL + endpoint, headers=headers, data=json.dumps(body)
        )
    else:
        raise ValueError(f"Unsupported HttpMethod: {request_type}")

    print(json.dumps(response.json(), indent=4, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("endpoint", help="The name of the endpoint, e.g. reports")
    parser.add_argument(
        "body",
        help="A JSON formatted string of params or data "
        + 'to pass to the endpoint, e.g. \'{"user_id":0,"agency_id": 1}\'',
    )
    parser.add_argument(
        "request_type_str", help="The name of the request type, e.g post"
    )
    args = parser.parse_args()
    make_request_to_api(
        endpoint=args.endpoint,
        body=json.loads(args.body),
        request_type_str=args.request_type_str,
    )
