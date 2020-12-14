# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Gets the IAP client id for a given airflow instance"""

import json
import sys
import urllib.parse

import requests


def get_iap_client_id(airflow_uri: str) -> str:
    redirect_response = requests.get(airflow_uri, allow_redirects=False)
    redirect_location = redirect_response.headers['location']

    # Extract the client_id query parameter from the redirect.
    parsed = urllib.parse.urlparse(redirect_location)
    query_string = urllib.parse.parse_qs(parsed.query)
    return query_string['client_id'][0]


def main() -> None:
    json_input = json.load(sys.stdin)
    client_id = get_iap_client_id(json_input['airflow_uri'])
    json.dump({'iap_client_id': client_id}, sys.stdout)


if __name__ == '__main__':
    main()
