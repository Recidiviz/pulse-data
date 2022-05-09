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
"""Wrapper around the Auth0 Client
"""
from typing import Dict, List, Optional, TypedDict

from auth0.v3.authentication import GetToken
from auth0.v3.management import Auth0
from auth0.v3.management.rest import RestClientOptions
from ratelimit import limits, sleep_and_retry

from recidiviz.utils import secrets

AUTH0_QPS_LIMIT = 2

Auth0AppMetadata = TypedDict(
    "Auth0AppMetadata",
    {
        "allowed_supervision_location_ids": List[str],
        "allowed_supervision_location_level": Optional[str],
        "can_access_leadership_dashboard": bool,
        "can_access_case_triage": bool,
        "should_see_beta_charts": bool,
        "routes": Optional[dict],
        "user_hash": str,
    },
)

Auth0User = TypedDict(
    "Auth0User", {"email": str, "user_id": str, "app_metadata": Auth0AppMetadata}
)

# The max results the API allows per page is 50, but the lucene query can become too long, so we limit it by 25.
MAX_RESULTS_PER_PAGE = 25
MAX_RATE_LIMIT_RETRIES = 10
REST_REQUEST_TIMEOUT = 15.0


class Auth0Client:
    """Wrapper around the Auth0 Client for interacting with the Auth0 Management API"""

    def __init__(self) -> None:
        domain = secrets.get_secret("auth0_api_domain")
        client_id = secrets.get_secret("auth0_api_client_id")
        client_secret = secrets.get_secret("auth0_api_client_secret")

        if not domain:
            raise ValueError("Missing required domain for Auth0 Management API")

        if not client_id:
            raise ValueError("Missing required client_id for Auth0 Management API")

        if not client_secret:
            raise ValueError("Missing required client_secret for Auth0 Management API")

        self._domain = domain
        self._client_id = client_id
        self._client_secret = client_secret
        self._rest_options = RestClientOptions(
            retries=MAX_RATE_LIMIT_RETRIES, timeout=REST_REQUEST_TIMEOUT
        )

        self.audience = f"https://{self._domain}/api/v2/"
        self.client = Auth0(
            self._domain, self.access_token, rest_options=self._rest_options
        )

    def get_token(self) -> Dict[str, str]:
        """Makes a request to the Auth0 Management API for an access token using the client credentials
        authorization flow."""
        return GetToken(self._domain).client_credentials(
            client_id=self._client_id,
            client_secret=self._client_secret,
            audience=self.audience,
        )

    @property
    def access_token(self) -> str:
        token = self.get_token()
        return token["access_token"]

    def get_all_users_by_email_addresses(
        self, email_addresses: List[str]
    ) -> List[Auth0User]:
        """Given a list of email addresses, it returns a list of Auth0 users with the user_id and email address for each
        user."""
        all_users = []
        email_addresses_copy = email_addresses.copy()
        while email_addresses_copy:
            emails_to_query = email_addresses_copy[0:MAX_RESULTS_PER_PAGE]
            del email_addresses_copy[0:MAX_RESULTS_PER_PAGE]
            lucene_query = self._create_search_users_by_email_query(emails_to_query)
            response = self.client.users.list(
                per_page=MAX_RESULTS_PER_PAGE,
                fields=["user_id", "email", "app_metadata"],
                q=lucene_query,
            )
            all_users.extend(response["users"])
        return all_users

    @sleep_and_retry
    @limits(calls=AUTH0_QPS_LIMIT, period=1)
    def update_user_app_metadata(
        self, user_id: str, app_metadata: Auth0AppMetadata
    ) -> None:
        """Updates a single Auth0 user's app_metadata field. Root-level properties are merged, and any nested properties
        are replaced. To delete an app_metadata property, send None as the value. To delete the app_metadata entirely,
        send an empty dictionary.

        This function is limited to two calls per second. Any calls beyond that will cause the function to sleep until
        it is safe to call again."""
        return self.client.users.update(id=user_id, body={"app_metadata": app_metadata})

    @staticmethod
    def _create_search_users_by_email_query(email_addresses: List[str]) -> str:
        """Returns a query that will search the email field, formatted as a lucene query string."""
        query_fields = []
        for email in email_addresses:
            query_fields.append(f'email: "{email}"')
        return " or ".join(query_fields)
