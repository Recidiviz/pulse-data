# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Client for interacting with the Intercom API"""

from datetime import datetime, timezone
from functools import cached_property
from typing import Any

import attr
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from recidiviz.intercom.types import (
    IntercomExportJobResponse,
    IntercomJobStatus,
    IntercomSearchEndpointResponse,
    IntercomSearchEndpointType,
    IntercomTicket,
    IntercomTicketResponse,
)
from recidiviz.utils.secrets import get_secret

DEFAULT_API_REQUEST_TIMEOUT = 10.0
# Number of tickets to fetch per page (max 150, per the Intercom API)
DEFAULT_PER_PAGE = 50
# Constants for querying the search endpoints
QUERY = "query"
OPERATOR = "operator"
AND = "AND"
VALUE = "value"
PAGINATION = "pagination"
PER_PAGE = "per_page"
STARTING_AFTER = "starting_after"
TICKETS = "tickets"
PAGES = "pages"
NEXT = "next"
UPDATED_AT = "updated_at"
FIELD = "field"
CONTACTS = "contacts"
DATA = "data"
CREATED_AT = "created_at"


@attr.define
class IntercomAPIClient:
    """Handles Intercom API interactions"""

    _auth_token: str | None = attr.ib(init=False)
    _BASE_URL = "https://api.intercom.io"

    def __attrs_post_init__(self) -> None:
        self._auth_token = get_secret("intercom_rir_auth_token")

    @cached_property
    def _session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {self._auth_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Intercom-Version": "2.11",
            }
        )
        return session

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def create_ticket(
        self, ticket_type_id: int, title: str, description: str, requester_email: str
    ) -> IntercomTicketResponse:
        url = f"{self._BASE_URL}/tickets"
        ticket_payload = IntercomTicket(
            ticket_type_id,
            requester_email,
            title,
            description,
        ).to_dict()

        response = self._session.post(
            url, json=ticket_payload, timeout=DEFAULT_API_REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return IntercomTicketResponse(id=response.json()["id"])

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    # TODO(OBT-27442): Change args to start_datetime_inclusive and end_datetime_inclusive
    def create_data_export(
        self, created_at_after: datetime, created_at_before: datetime
    ) -> IntercomExportJobResponse:
        """Creates a job to export message delivery and engagement statistics for outbound content (Emails, Posts, Custom Bots, Surveys, Tours, Series, and more) sent in a given timeframe.
        The exported data includes who received each message, when they received it, and how they engaged with it (opens, clicks, replies, completions, dismissals, unsubscribes, and bounces).

        It should be noted that the timeframe only includes messages sent during the time period and not messages that were only updated during this period.

        https://developers.intercom.com/docs/references/rest-api/api.intercom.io/data-export/createdataexport

                Args:
                    created_at_after: UTC datetime for start of date range, inclusive
                    created_at_before: UTC datetime for end of date range, inclusive

                Returns:
                    IntercomExportJobResponse with job_identifier and initial status
        """
        if created_at_after >= created_at_before:
            raise ValueError(
                "created_at_after must be before created_at_before. "
                f"Got {created_at_after} and {created_at_before}"
            )
        if (
            created_at_after.tzinfo is not timezone.utc
            or created_at_before.tzinfo is not timezone.utc
        ):
            raise ValueError(
                "created_at_after and created_at_before must be timezone-aware UTC datetimes. "
                f"Got {created_at_after} and {created_at_before}"
            )

        url = f"{self._BASE_URL}/export/content/data"
        payload = {
            "created_at_after": int(created_at_after.timestamp()),
            "created_at_before": int(created_at_before.timestamp()),
        }

        response = self._session.post(
            url, json=payload, timeout=DEFAULT_API_REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        return IntercomExportJobResponse(
            job_identifier=data["job_identifier"],
            status=IntercomJobStatus(data["status"]),
            download_url=data["download_url"],
            download_expires_at=data["download_expires_at"],
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def get_export_status(self, job_identifier: str) -> IntercomExportJobResponse:
        """Checks the status of an export job.

        Args:
            job_identifier: The job identifier returned from create_data_export

        Returns:
            IntercomExportJobResponse with current status and download_url if completed
        """
        url = f"{self._BASE_URL}/export/content/data/{job_identifier}"

        response = self._session.get(url, timeout=DEFAULT_API_REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        return IntercomExportJobResponse(
            job_identifier=data["job_identifier"],
            status=IntercomJobStatus(data["status"]),
            download_url=data["download_url"],
            download_expires_at=data["download_expires_at"],
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def download_export_data(self, job_identifier: str) -> bytes:
        """Downloads the export data as a zip file.

        Args:
            job_identifier: The job identifier returned from create_data_export

        Returns:
            Raw bytes of the zip file containing CSV exports
        """
        url = f"{self._BASE_URL}/download/content/data/{job_identifier}"

        # Create a temporary session with octet-stream accept header
        response = self._session.get(
            url,
            headers={"Accept": "application/octet-stream"},
            timeout=30.0,
        )
        response.raise_for_status()
        return response.content

    def get_search_endpoint_response(
        self,
        endpoint_type: IntercomSearchEndpointType,
        query_filters: list[dict[str, str]],
        response_data_key: str,
        next_cursor: str | None = None,
    ) -> IntercomSearchEndpointResponse:
        """Queries an Intercom API Search endpoint and returns an
        IntercomSearchEndpointResponse object with a list of JSON from the given search endpoint.

        Args:
            endpoint_type: The search endpoint to be queried (https://api.intercom.io/<endpoint_type>/search)
            query_filters: Filters to query the endpoint on
            response_data_key: Key that contains the data from the requested endpoint in the Intercom API response
            per_page: Number of tickets to fetch per page (max 150, per the Intercom API)
            next_cursor: The cursor to use in the next request to get the next page of results

        Returns:
            List of JSON from the given endpoint
        """

        url = f"{self._BASE_URL}/{endpoint_type.value}/search"

        payload: dict = {
            QUERY: {
                OPERATOR: AND,
                VALUE: query_filters,
            },
            PAGINATION: {
                PER_PAGE: DEFAULT_PER_PAGE,
            },
        }

        if next_cursor:
            payload[PAGINATION][STARTING_AFTER] = next_cursor

        response = self._session.post(
            url, json=payload, timeout=DEFAULT_API_REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()

        # The tickets/search enpoint and contacts/search endpoints return different JSON structure
        data_list = data[response_data_key]

        pages = data[PAGES]
        # If there are no more pages, the data["pages"]["next"] key does not exist
        next_page = pages.get(NEXT, None)
        next_cursor = next_page[STARTING_AFTER] if next_page else None

        return IntercomSearchEndpointResponse(
            endpoint_type=endpoint_type, data=data_list, next_cursor=next_cursor
        )

    def search_endpoint(
        self,
        endpoint_type: IntercomSearchEndpointType,
        query_filters: list[dict[str, str]],
        response_data_key: str,
    ) -> list[dict[str, Any]]:
        """Fetches data from Intercom using a search endpoint, exhausting the cursor to get all of the data.

        Args:
            endpoint_type: The search endpoint to be queried (https://api.intercom.io/<endpoint_type>/search)
            query_filters: Filters to query the endpoint on
            response_data_key: Key that contains the data from the requested endpoint in the Intercom API response
            per_page: Number of tickets to fetch per page (max 150, per the Intercom API)

        Returns:
            List of Intercom inbound support tickets JSON
        """

        search_endpoint_response = self.get_search_endpoint_response(
            endpoint_type=endpoint_type,
            query_filters=query_filters,
            response_data_key=response_data_key,
        )
        returned_data = search_endpoint_response.data

        while search_endpoint_response.next_cursor is not None:
            search_endpoint_response = self.get_search_endpoint_response(
                endpoint_type=endpoint_type,
                query_filters=query_filters,
                response_data_key=response_data_key,
                next_cursor=search_endpoint_response.next_cursor,
            )
            returned_data.extend(search_endpoint_response.data)

        return returned_data

    def get_tickets(
        self, updated_before: datetime, updated_after: datetime
    ) -> list[dict[str, Any]]:
        """Fetches tickets from Intercom using the search tickets endpoint.

        Args:
            updated_before: Datetime to filter tickets updated before this time
            updated_after: Datetime to filter tickets updated after this time

        Returns:
            List of Intercom inbound support tickets JSON
        """

        query_filters = [
            {
                FIELD: UPDATED_AT,
                OPERATOR: ">",
                VALUE: str(updated_after.timestamp()),
            },
            {
                FIELD: UPDATED_AT,
                OPERATOR: "<",
                VALUE: str(updated_before.timestamp()),
            },
        ]

        tickets = self.search_endpoint(
            query_filters=query_filters,
            endpoint_type=IntercomSearchEndpointType.TICKETS,
            response_data_key=TICKETS,
        )
        return tickets

    def get_contacts(
        self, created_before: datetime, created_after: datetime
    ) -> list[dict[str, Any]]:
        """Fetches contacts from Intercom using the search contacts endpoint.

        Args:
            created_before: Datetime to filter contacts created before this time
            created_after: Datetime to filter contacts created after this time

        Returns:
            List of Intercom contacts JSON
        """

        query_filters = [
            {
                FIELD: CREATED_AT,
                OPERATOR: ">",
                VALUE: str(created_after.timestamp()),
            },
            {
                FIELD: CREATED_AT,
                OPERATOR: "<",
                VALUE: str(created_before.timestamp()),
            },
        ]

        contacts = self.search_endpoint(
            query_filters=query_filters,
            endpoint_type=IntercomSearchEndpointType.CONTACTS,
            response_data_key=DATA,
        )
        return contacts
