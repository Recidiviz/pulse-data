# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
Tool for making test request to the Outliers APIs.

To create the required databases and add data to them from staging, use the script in
recidiviz/tools/outliers/load_local_db.py

To run locally, run: docker-compose -f docker-compose.yaml -f docker-compose.case-triage.yaml up --remove-orphans

Example usage:
python -m recidiviz.tools.outliers.request_api --target_env staging --token abc123 --endpoint US_MI/supervisors
"""
import argparse
import json

import requests

LOCALHOST_URL = "http://localhost:5000/"
STAGING_URL = "https://app-staging.recidiviz.org/"


def request_api(
    target_env: str,
    token: str,
    endpoint: str,
) -> None:
    url = STAGING_URL if target_env == "staging" else LOCALHOST_URL

    s = requests.session()
    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json",
        "Origin": "http://localhost:3000",
    }
    response = s.get(url + f"outliers/{endpoint}", headers=headers)
    print(response.status_code)
    print(json.dumps(response.json(), indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target_env",
        help="What environment to point the request to",
        default="dev",
        choices=["dev", "staging"],
    )
    parser.add_argument("--token", help="Valid auth0 token to use")
    parser.add_argument("--endpoint", help="Endpoint to make a request to.")

    args = parser.parse_args()
    request_api(args.target_env, args.token, args.endpoint)
