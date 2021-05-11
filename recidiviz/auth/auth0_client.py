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
import math
from typing import List, Dict, TypedDict
from auth0.v3.authentication import GetToken
from auth0.v3.management import Auth0

from recidiviz.utils import secrets

Auth0AppMetadata = TypedDict(
    "Auth0AppMetadata",
    {
        "allowed_supervision_location_ids": List[str],
        "allowed_supervision_location_level": str,
    },
)

Auth0User = TypedDict(
    "Auth0User", {"email": str, "user_id": str, "app_metadata": Auth0AppMetadata}
)


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

        self.audience = f"https://{self._domain}/api/v2/"
        self.client = Auth0(self._domain, self.access_token)

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
        user. The max results the API allows per page is 50, so this collects all of the users per page."""
        all_users = []
        max_results_per_page = 50
        num_pages = math.ceil(len(email_addresses) / max_results_per_page)
        lucene_query = self._create_search_users_by_email_query(email_addresses)
        for page in range(num_pages):
            response = self.client.users.list(
                page=page,
                per_page=max_results_per_page,
                fields=["user_id", "email", "app_metadata"],
                q=lucene_query,
            )
            all_users.extend(response["users"])
        return all_users

    def update_user_app_metadata(
        self,
        user_id: str,
        app_metadata: Auth0AppMetadata,
    ) -> None:
        """Updates a single Auth0 user's app_metadata field. Root-level properties are merged, and any nested properties
        are replaced. To delete an app_metadata property, send None as the value. To delete the app_metadata entirely,
        send an empty dictionary."""
        return self.client.users.update(id=user_id, body={"app_metadata": app_metadata})

    @staticmethod
    def _create_search_users_by_email_query(email_addresses: List[str]) -> str:
        """Returns a query that will search the email field, formatted as a lucene query string."""
        query_fields = []
        for email in email_addresses:
            query_fields.append(f'email: "{email}"')
        return " or ".join(query_fields)
