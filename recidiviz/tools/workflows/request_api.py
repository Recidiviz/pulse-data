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
"""
Tool for making test request to the Workflows API.

Currently, this is specific to external requests to TOMIS but can later be generalized for other
Workflows-related external requests.

In order to test requests to TOMIS, this script should be run with the --target_env as 'staging' in order
for the request to be made from the static IP address expected by TOMIS.

Example usage:
python -m recidiviz.tools.workflows.request_api staging a1b2c3 complete_request
"""
import argparse
import json
from http import HTTPStatus

import requests

from recidiviz.tools.workflows.fixtures.tomis_contact_notes import (
    build_notes,
    note_name_to_objs,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.secrets import get_secret

LOCALHOST_URL = "http://localhost:5000/"
STAGING_URL = "https://app-staging.recidiviz.org/"


def insert_contact_note(
    target_env: str, token: str, fixture_name: str, timeout_secs: int
) -> None:
    """Used to make a test request to TOMIS"""
    if not fixture_name in note_name_to_objs:
        raise Exception(
            f"fixture name not found, options are {note_name_to_objs.keys()}"
        )

    offender_id = get_secret("workflows_us_tn_test_offender_id")
    user_id = get_secret("workflows_us_tn_test_user_id")

    if offender_id is None or user_id is None:
        raise Exception("Missing OffenderId and/or UserId secret")

    url = STAGING_URL if target_env == "staging" else LOCALHOST_URL

    # Get a valid CSRF token
    s = requests.session()
    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    response = s.get(url + "workflows/US_TN/init", headers=headers)
    if response.status_code != HTTPStatus.OK:
        raise Exception(
            f"Could not generate CSRF token. Got status_code {response.status_code} on /init"
        )
    headers["X-CSRF-Token"] = response.json()["csrf"]
    headers["Referer"] = "https://app-staging.recidiviz.org"

    pages = note_name_to_objs[fixture_name]
    test_case_number = list(note_name_to_objs.keys()).index(fixture_name)
    total_duration = 0
    for page in build_notes(pages, test_case_number, offender_id, user_id):
        data = {
            "isTest": True,
            "fixture": page,
            "env": target_env,
            "timeoutSecs": timeout_secs,
        }

        print(f"Sending request with data:\n {json.dumps(page, indent=2)}")
        response = s.post(
            url + "workflows/external_request/US_TN/insert_contact_note",
            headers=headers,
            json=data,
        )
        resp_json = response.json()
        print(resp_json)
        if "duration" in resp_json:
            total_duration += resp_json["duration"]

    print(f"Total duration: {total_duration}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target_env", help="What environment to point the request to", default="dev"
    )
    parser.add_argument("--token", help="Valid auth0 token to use")
    parser.add_argument(
        "--fixture_name", help="Name of the fixture to use in the test request to TOMIS"
    )
    parser.add_argument(
        "--timeout_secs", help="Timeout to pass in put request to TOMIS", default=360
    )

    args = parser.parse_args()
    with local_project_id_override("recidiviz-staging"):
        insert_contact_note(
            args.target_env, args.token, args.fixture_name, int(args.timeout_secs)
        )
