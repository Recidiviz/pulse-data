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

import attrs
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from recidiviz.intercom.types import (
    IntercomExportJobResponse,
    IntercomTicket,
    IntercomTicketResponse,
)
from recidiviz.utils.secrets import get_secret

DEFAULT_API_REQUEST_TIMEOUT = 10.0


@attrs.define
class IntercomAPIClient:
    """Handles Intercom API interactions"""

    _AUTH_TOKEN = get_secret("intercom_rir_auth_token")
    _BASE_URL = "https://api.intercom.io"

    @cached_property
    def _session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {self._AUTH_TOKEN}",
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
            status=data["status"],
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
            status=data["status"],
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
