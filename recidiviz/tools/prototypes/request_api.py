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

python -m recidiviz.tools.prototypes.request_api search '{"query":"housing updates", "user_id":"fake_id", "state_code":"US_ID"}' get --target_env dev

python -m recidiviz.tools.prototypes.request_api search '{"query":"housing updates", "user_id":"fake_id", "state_code":"US_ME"}' get --target_env staging --token eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik1qY3dRakJFUmpnMFJFUTJRakJFTWpZMU5rTXhNRVU0UkRSQk1rUkVPVFpGTUVRNE56SXdNdyJ9.eyJodHRwczovL2Rhc2hib2FyZC5yZWNpZGl2aXoub3JnL2FwcF9tZXRhZGF0YSI6eyJhbGxvd2VkU3RhdGVzIjpbIlVTX09aIiwiVVNfUEEiLCJVU19DQSIsIlVTX0NPIiwiVVNfTkMiLCJVU19PUiIsIlVTX05EIiwiVVNfTU8iLCJVU19JWCIsIlVTX0lEIiwiVVNfVE4iLCJVU19NRSIsIlVTX0FSIl0sInN0YXRlQ29kZSI6InJlY2lkaXZpeiIsInN0YXRlX2NvZGUiOiJyZWNpZGl2aXoifSwiaHR0cHM6Ly9kYXNoYm9hcmQucmVjaWRpdml6Lm9yZy9yZWdpc3RyYXRpb25fZGF0ZSI6IjIwMjQtMDctMTVUMjI6MzQ6MzIuMTUyWiIsImh0dHBzOi8vZGFzaGJvYXJkLnJlY2lkaXZpei5vcmcvZW1haWxfYWRkcmVzcyI6ImJyYW5kb25AcmVjaWRpdml6Lm9yZyIsImlzcyI6Imh0dHBzOi8vbG9naW4tc3RhZ2luZy5yZWNpZGl2aXoub3JnLyIsInN1YiI6Imdvb2dsZS1hcHBzfGJyYW5kb25AcmVjaWRpdml6Lm9yZyIsImF1ZCI6WyJodHRwczovL2FwaS1kZXYucmVjaWRpdml6Lm9yZyIsImh0dHBzOi8vcmVjaWRpdml6LWRldi5hdXRoMC5jb20vdXNlcmluZm8iXSwiaWF0IjoxNzI0NzY3MjM1LCJleHAiOjE3MjQ3NjgxMzUsInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZW1haWwiLCJhenAiOiJqbTRpZTZFOFJoS0tXRTZXbHU0YnkxNlZSN3dhZW0zYiIsInBlcm1pc3Npb25zIjpbXX0.j14-8LN5DyRVNRQNFKE7JfjlSMfkx8z4fpW6fP9BeyiREnRFLqhpGi-tD-N9BrVYSXGq1_p5_EF1PpjS1McODIiBlsctckF1H7C-ac5rGPA8xIstumrCZ6ShaMYVzU7sq9osg_1X3jpkxOFMQmvCc1XTgHX_Azt0KI9bNtrHOn7jwHX11D3pRbQZe3w6X2UbCVSATbeuGGajG6Ou0vCSMbU_W-jP_bXjrxpsuAisMu7ravzAzjff0mQRqJImMGLFMTPEYDdyWJmX05E6uQk2aJkEJsIgCR3LfcU3kkj0MlXltqUmXyaXMSnumAiQxXTLnmRP3WUq74m9kzi6WQL-nQ

python -m recidiviz.tools.prototypes.request_api search '{"query":"housing updates", "user_id":"fake_id", "state_code":"US_ID"}' get --target_env prod --token eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlF6UkdOa0pHTURjME5VUTNRVEE0TjBKRFFrVXdOalEwTVRrM05ERkVPVGhEUWpRMFF6a3lNdyJ9.eyJodHRwczovL2Rhc2hib2FyZC5yZWNpZGl2aXoub3JnL2FwcF9tZXRhZGF0YSI6eyJhbGxvd2VkU3RhdGVzIjpbIlVTX09aIiwiVVNfUEEiLCJVU19DQSIsIlVTX0NPIiwiVVNfTkMiLCJVU19PUiIsIlVTX05EIiwiVVNfTU8iLCJVU19JWCIsIlVTX0lEIiwiVVNfVE4iLCJVU19NRSIsIlVTX0FSIl0sInN0YXRlQ29kZSI6InJlY2lkaXZpeiIsInN0YXRlX2NvZGUiOiJyZWNpZGl2aXoifSwiaHR0cHM6Ly9kYXNoYm9hcmQucmVjaWRpdml6Lm9yZy9yZWdpc3RyYXRpb25fZGF0ZSI6IjIwMjQtMDctMThUMTQ6NDk6MTAuMDEzWiIsImh0dHBzOi8vZGFzaGJvYXJkLnJlY2lkaXZpei5vcmcvZW1haWxfYWRkcmVzcyI6ImJyYW5kb25AcmVjaWRpdml6Lm9yZyIsImlzcyI6Imh0dHBzOi8vbG9naW4ucmVjaWRpdml6Lm9yZy8iLCJzdWIiOiJnb29nbGUtYXBwc3xicmFuZG9uQHJlY2lkaXZpei5vcmciLCJhdWQiOlsiaHR0cHM6Ly9hcGkucmVjaWRpdml6Lm9yZyIsImh0dHBzOi8vcmVjaWRpdml6LmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE3MjQ3NjczMjAsImV4cCI6MTcyNDc2ODIyMCwic2NvcGUiOiJvcGVuaWQgcHJvZmlsZSBlbWFpbCIsImF6cCI6IlRSTU1VM0g1UWZjSXVUTGU3b3c1dEcyOURmN1FqM0ZIIn0.nDfXAKM_uZSZbBWICIyvVxhstrmN84PwNV7GW9Rpym4vq2BbovFccX2S14qIisqGJ2HZaDoogR-Z15GQpmR_SLiRtEb-ccmVkT7xlkwD-JZqOzKbRPKwrQYyObQ52N7ppYT8mjV37ZX73HxFO2uQW2KfkPLdRRYjy0QcrXcgOQ7RJdzk8EcUMMGQMqlDgblU5ttgG7bF0bOoz56VhqFFb66Vz-Fhg1AATb94Kg90lVI5BLNoUTfprKFDwBEUoA1crvEMl0X9BOx5tNtCUN0oL-9iDCk4nf5JZz__6fzpOiCHeffTTRo7PFrDrPGWWxfp5cNO0S8ayZ5lK1RewgMwmQ

Optional params:
    --token: The user can provide a request token which would override the M2M token. A
        token is necessary for hitting the staging and production endpoints.

"""
import argparse
import json
from typing import Dict, Optional

import requests

from recidiviz.utils.secrets import get_secret_from_local_directory

LOCALHOST_URL = "http://localhost:8080/"
STAGING_URL = "https://prototypes-qqec6jbn6a-uc.a.run.app/"
PROD_URL = "https://prototypes-buq47yc7fa-uc.a.run.app/"

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

    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json",
        "Origin": "http://localhost:3000",
    }

    # Make the request
    if target_env == "staging":
        url = STAGING_URL
    elif target_env == "prod":
        url = PROD_URL
    else:
        url = LOCALHOST_URL
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
        choices=["dev", "staging", "prod"],
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
