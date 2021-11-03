# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""This file contains all of the relevant helpers for cloud functions.

Mostly copied from:
https://cloud.google.com/iap/docs/authentication-howto#iap_make_request-python
"""
import urllib.parse
from typing import Any, List

import google.auth
import google.auth.app_engine
import google.auth.compute_engine.credentials
import google.auth.iam
import google.oauth2.credentials
import google.oauth2.service_account
import requests
from google.auth import crypt
from google.auth.transport.requests import Request

IAP_CLIENT_ID = {
    "recidiviz-staging": (
        "984160736970-flbivauv2l7sccjsppe34p7436l6890m.apps." "googleusercontent.com"
    ),
    "recidiviz-123": (
        "688733534196-uol4tvqcb345md66joje9gfgm26ufqj6.apps." "googleusercontent.com"
    ),
}

GCP_PROJECT_ID_KEY = "GCP_PROJECT"

_IAM_SCOPE = "https://www.googleapis.com/auth/iam"
_OAUTH_TOKEN_URI = "https://www.googleapis.com/oauth2/v4/token"

# pylint: disable=protected-access


def make_iap_request(
    url: str, client_id: str, method: str = "GET", **kwargs: Any
) -> requests.Response:
    """Makes a request to an application protected by Identity-Aware Proxy.

    Args:
      url: The Identity-Aware Proxy-protected URL to fetch.
      client_id: The client ID used by Identity-Aware Proxy.
      method: The request method to use
              ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py

    Returns:
      The page body, or raises an exception if the page couldn't be retrieved.
    """
    # Figure out what environment we're running in and get some preliminary
    # information about the service account.
    bootstrap_credentials, _ = google.auth.default(scopes=[_IAM_SCOPE])
    if isinstance(bootstrap_credentials, google.oauth2.credentials.Credentials):
        raise Exception("make_iap_request is only supported for service " "accounts.")

    # For service accounts using the Compute Engine metadata service,
    # service_account_email isn't available until refresh is called.
    bootstrap_credentials.refresh(Request())

    signer_email = bootstrap_credentials.service_account_email
    if isinstance(
        bootstrap_credentials, google.auth.compute_engine.credentials.Credentials
    ):
        signer: crypt.Signer = google.auth.iam.Signer(
            Request(), bootstrap_credentials, signer_email
        )
    else:
        # A Signer object can sign a JWT using the service accounts key.
        signer = bootstrap_credentials.signer

    # Construct OAuth 2.0 service account credentials using the signer
    # and email acquired from the bootstrap credentials.
    service_account_credentials = google.oauth2.service_account.Credentials(
        signer,
        signer_email,
        token_uri=_OAUTH_TOKEN_URI,
        additional_claims={"target_audience": client_id},
    )

    # service_account_credentials gives us a JWT signed by the service
    # account. Next, we use that to obtain an OpenID Connect token,
    # which is a JWT signed by Google.
    google_open_id_connect_token = get_google_open_id_connect_token(
        service_account_credentials
    )

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    response = requests.request(
        method,
        url,
        headers={"Authorization": f"Bearer {google_open_id_connect_token}"},
        **kwargs,
    )
    if response.status_code == 403:
        raise Exception(
            f"Service account {signer_email} does not have permission to "
            "access the IAP-protected application."
        )
    if response.status_code != 200:
        raise Exception(
            f"Bad response from application: {repr(response.status_code)} / "
            f"{repr(response.headers)} / {repr(response.text)}"
        )
    return response


def get_google_open_id_connect_token(
    service_account_credentials: google.oauth2.credentials.Credentials,
) -> str:
    """Get an OpenID Connect token issued by Google for the service account."""

    service_account_jwt = (
        service_account_credentials._make_authorization_grant_assertion()
    )
    request = google.auth.transport.requests.Request()
    body = {
        "assertion": service_account_jwt,
        "grant_type": google.oauth2._client._JWT_GRANT_TYPE,
    }
    token_response = google.oauth2._client._token_endpoint_request(
        request, _OAUTH_TOKEN_URI, body
    )
    return token_response["id_token"]


def build_query_param_string(
    request_params: dict, accepted_query_params: List[str]
) -> str:
    """Given a dict of request params from the CF event JSON, it returns a query string for a URL endpoint for the
    request params that are included in the `accepted_query_params` list.
    If the param value is a list, it will add a query param for each value in the list.
    If a request param key is not accepted by the endpoint, it will raise a KeyError.
    """
    query_tuples = []
    for param_key, param_value in request_params.items():
        if param_key not in accepted_query_params:
            raise KeyError(
                f"Unexpected key in request: [{param_key}]. "
                f"Expected one of the following: {accepted_query_params}"
            )
        if isinstance(param_value, list):
            for val in param_value:
                query_tuples.append((param_key, val))
        if isinstance(param_value, str):
            query_tuples.append((param_key, param_value))
    return f"?{urllib.parse.urlencode(query_tuples)}"
