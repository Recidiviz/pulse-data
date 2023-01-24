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
python -m recidiviz.tools.workflows.request_api --target_env staging --page_names ten_lines one_line --token abc123
"""
import argparse
import json
from datetime import datetime
from http import HTTPStatus
from typing import Dict, List, Optional

import requests

from recidiviz.tools.workflows.fixtures.tomis_contact_notes import page_name_to_page
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.secrets import get_secret

LOCALHOST_URL = "http://localhost:5000/"
STAGING_URL = "https://app-staging.recidiviz.org/"


def insert_contact_note(
    target_env: str,
    token: str,
    page_names: List[str],
    should_queue_task: Optional[bool],
    voters_rights_code: Optional[str],
) -> None:
    """Used to make a test request to TOMIS"""
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

    data = {
        "personExternalId": offender_id,
        "userId": user_id,
        "contactNoteDateTime": str(datetime.now()),
    }

    contact_note: Dict[int, List[str]] = {}
    for idx, page_name in enumerate(page_names):
        if page_name not in page_name_to_page:
            raise ValueError("Page fixture doesn't exist")

        contact_note[idx + 1] = page_name_to_page[page_name]

    data["contactNote"] = json.dumps(contact_note)

    if should_queue_task is not None:
        data["shouldQueueTask"] = str(should_queue_task)

    if voters_rights_code is not None:
        data["votersRightsCode"] = voters_rights_code

    print(f"Sending request with data:\n {json.dumps(data, indent=2)}")
    response = s.post(
        url + "workflows/external_request/US_TN/insert_tepe_contact_note",
        headers=headers,
        json=data,
    )
    resp_json = response.json()
    print(resp_json)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target_env", help="What environment to point the request to", default="dev"
    )
    parser.add_argument("--token", help="Valid auth0 token to use")
    parser.add_argument(
        "--page_names", nargs="+", help="List of names of pages to use in the request"
    )
    parser.add_argument(
        "--should_queue_task", help="Whether or not to use the CloudTask queue"
    )
    parser.add_argument(
        "--voters_rights_code",
        help="Optional argument to use as a second contact code. One of VRRE or VRRI",
    )

    args = parser.parse_args()
    with local_project_id_override("recidiviz-staging"):
        insert_contact_note(
            args.target_env,
            args.token,
            args.page_names,
            args.should_queue_task,
            args.voters_rights_code,
        )
