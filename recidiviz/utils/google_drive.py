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
Utils for working with Google Drive.

The first time `get_credentials` is run, you must download credentials.json from here:
https://console.developers.google.com/apis/credentials. These are long-lasting
credentials so put them in a permanent directory. Pass the path to that directory
as the directory parameter of `get_credentials`.
"""
import os
import pickle
from typing import List

from google.auth.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow


def get_credentials(
    directory: str,
    readonly: bool,
    scopes: List[str],
) -> Credentials:
    """
    Returns credentials to access Google Drive services.

    Params
    ------
    directory : str
        Local path to credentials.json

    readonly : bool
        If true, returns credentials with read-only permissions

    scopes : List[str]
        List of scopes (permissions) for the credentials to cover
    """
    creds = None

    if readonly:
        scopes = [f"{scope}.readonly" for scope in scopes]

    token_path = os.path.join(directory, "token.pickle")
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token_path):
        with open(token_path, "rb") as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                os.path.join(directory, "credentials.json"), scopes
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path, "wb") as token:
            pickle.dump(creds, token)

    return creds
