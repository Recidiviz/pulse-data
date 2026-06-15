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
"""Client for interacting with the Intercom APIs"""

from functools import cached_property

import attrs
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from recidiviz.intercom.types import IntercomTicket, IntercomTicketResponse
from recidiviz.utils.secrets import get_secret


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

        response = self._session.post(url, json=ticket_payload, timeout=10.0)
        response.raise_for_status()
        return IntercomTicketResponse(id=response.json().get("id", ""))
