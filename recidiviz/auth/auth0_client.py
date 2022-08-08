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
import logging
from functools import wraps
from http import HTTPStatus
from typing import Any, Callable, Dict, List, Optional, TypedDict, Union

from auth0.v3.authentication import GetToken
from auth0.v3.exceptions import Auth0Error
from auth0.v3.management import Auth0
from auth0.v3.rest import RestClientOptions
from ratelimit import limits, sleep_and_retry

from recidiviz.utils import secrets

AUTH0_QPS_LIMIT = 2


class CaseTriageAuth0AppMetadata(TypedDict):
    allowed_supervision_location_ids: List[str]
    allowed_supervision_location_level: Optional[str]
    can_access_leadership_dashboard: bool
    can_access_case_triage: bool
    should_see_beta_charts: bool
    routes: Optional[dict]
    user_hash: str


class JusticeCountsAuth0AppMetadata(TypedDict, total=False):
    has_seen_onboarding: Optional[Dict[str, bool]]
    agency_ids: List[int]


Auth0User = TypedDict(
    "Auth0User",
    {
        "email": str,
        "user_id": str,
        "name": str,
        "app_metadata": Union[
            JusticeCountsAuth0AppMetadata, CaseTriageAuth0AppMetadata
        ],
    },
)


# The max results the API allows per page is 50, but the lucene query can become too long, so we limit it by 25.
MAX_RESULTS_PER_PAGE = 25
MAX_RATE_LIMIT_RETRIES = 10
REST_REQUEST_TIMEOUT = 15.0


def _refresh_token_if_needed(fn: Callable) -> Callable:
    @wraps(fn)
    def decorated(
        self: "Auth0Client", *args: List[Any], **kwargs: Dict[str, Any]
    ) -> Callable:

        try:
            return fn(self, *args, **kwargs)
        except Auth0Error as e:
            if (
                e.status_code == HTTPStatus.UNAUTHORIZED
                and "Expired token" in e.message
            ):
                logging.info(
                    "Auth0 Management API token expired; "
                    "fetching new one and re-initializing the client."
                )
                self.refresh_client_with_new_token()
                return fn(self, *args, **kwargs)
            raise e

    return decorated


class Auth0Client:
    """Wrapper around the Auth0 Client for interacting with the Auth0 Management API"""

    def __init__(  # nosec
        self,
        domain_secret_name: str = "auth0_api_domain",
        client_id_secret_name: str = "auth0_api_client_id",
        client_secret_secret_name: str = "auth0_api_client_secret",
    ) -> None:
        domain = secrets.get_secret(domain_secret_name)
        client_id = secrets.get_secret(client_id_secret_name)
        client_secret = secrets.get_secret(client_secret_secret_name)

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
        self.refresh_client_with_new_token()

    def fetch_new_access_token(self) -> str:
        """Makes a request to the Auth0 Management API for an access token using the client credentials
        authorization flow."""
        token = GetToken(self._domain).client_credentials(
            client_id=self._client_id,
            client_secret=self._client_secret,
            audience=self.audience,
        )
        return token["access_token"]

    def refresh_client_with_new_token(self) -> None:
        self.client = Auth0(
            self._domain, self.fetch_new_access_token(), rest_options=self._rest_options
        )

    @_refresh_token_if_needed
    def get_user_by_id(self, user_id: str) -> Auth0User:
        """Given an Auth0 user id, return the corresponding user."""
        return self.client.users.get(id=user_id)

    @_refresh_token_if_needed
    def get_all_users(self) -> List[Auth0User]:
        """Return a list of all Auth0 users associated with this tenant."""
        all_users = []
        continue_fetching = True
        while continue_fetching:
            response = self.client.users.list(
                per_page=MAX_RESULTS_PER_PAGE,
                fields=["user_id", "name", "email", "app_metadata"],
            )
            users = response["users"]
            all_users.extend(users)
            continue_fetching = len(users) == MAX_RESULTS_PER_PAGE
        return all_users

    @_refresh_token_if_needed
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
    @_refresh_token_if_needed
    def update_user(
        self,
        user_id: str,
        name: Optional[str] = None,
        email: Optional[str] = None,
        email_verified: Optional[bool] = False,
        app_metadata: Optional[
            Union[JusticeCountsAuth0AppMetadata, CaseTriageAuth0AppMetadata]
        ] = None,
    ) -> Auth0User:
        """Updates a single Auth0 user's name, email, and app_metadata fields.
        This function is limited to two calls per second. Any calls beyond that will cause the function to sleep until
        it is safe to call again."""

        body: Dict[str, Any] = {}
        if email is not None:
            body["email"] = email
            body["email_verified"] = email_verified

        if name is not None:
            body["name"] = name

        if app_metadata is not None:
            body["app_metadata"] = app_metadata

        return self.client.users.update(id=user_id, body=body)

    @sleep_and_retry
    @limits(calls=AUTH0_QPS_LIMIT, period=1)
    @_refresh_token_if_needed
    def send_verification_email(
        self,
        user_id: str,
    ) -> None:
        """Sends verification email to user."""

        return self.client.jobs.send_verification_email(body={"user_id": user_id})

    @sleep_and_retry
    @_refresh_token_if_needed
    @limits(calls=AUTH0_QPS_LIMIT, period=1)
    def update_user_app_metadata(
        self,
        user_id: str,
        app_metadata: Union[JusticeCountsAuth0AppMetadata, CaseTriageAuth0AppMetadata],
    ) -> Auth0User:
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
