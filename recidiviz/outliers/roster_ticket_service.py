# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Roster Ticket Service that handles ticket requests between Insights and Intercom"""

from functools import cached_property
from typing import Callable, Iterable, List, TypeVar, Union

import attrs
import requests
from more_itertools import flatten
from tenacity import retry, stop_after_attempt, wait_exponential

from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.outliers.types import (
    IntercomTicket,
    IntercomTicketResponse,
    OutliersProductConfiguration,
    PersonName,
    RosterChangeRequestResponseSchema,
    RosterChangeType,
)
from recidiviz.persistence.database.schema.insights.schema import (
    SupervisionOfficer,
    SupervisionOfficerSupervisor,
)
from recidiviz.utils.secrets import get_secret

REPORT_INCORRECT_ROSTER_TICKET_TYPE = 1
T = TypeVar("T")


@attrs.define
class IntercomAPIClient:
    """Handles Intercom API interactions for roster ticketing using requests."""

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
        self, title: str, description: str, requester_email: str
    ) -> IntercomTicketResponse:
        url = f"{self._BASE_URL}/tickets"
        ticket_payload = IntercomTicket(
            REPORT_INCORRECT_ROSTER_TICKET_TYPE,
            requester_email,
            title,
            description,
        ).to_dict()

        response = self._session.post(url, json=ticket_payload, timeout=10.0)
        response.raise_for_status()
        return IntercomTicketResponse(id=response.json().get("id", ""))


@attrs.define(kw_only=True)
class RosterTicketService:
    """Handles business logic for roster change ticket creation."""

    querier: OutliersQuerier
    intercom_api_client: IntercomAPIClient = IntercomAPIClient()

    @cached_property
    def _get_querier_product_config(self) -> OutliersProductConfiguration:
        return self.querier.get_product_configuration()

    def _fetch_entities(
        self, ids: Iterable[str], fetch_fn: Callable[[List[str]], List[T]], label: str
    ) -> List[T]:
        """Generic entity fetcher with validation."""
        entities = fetch_fn(list(ids))
        found_ids = {getattr(e, "external_id") for e in entities}
        missing = set(ids) - found_ids
        if missing:
            raise ValueError(f"{label.capitalize()}(s) not found: {', '.join(missing)}")
        return entities

    def _build_ticket_description(
        self,
        requester_name: str,
        target_name: str,
        change_type: RosterChangeType,
        note: str,
        supervisors: List[SupervisionOfficerSupervisor],
        officers: List[SupervisionOfficer],
    ) -> str:
        """Constructs a formatted ticket description."""

        supervisor_map = {s.external_id: s for s in supervisors}
        officer_to_supervisors = {
            o: [
                supervisor_map[s_id]
                for s_id in o.supervisor_external_ids
                if s_id in supervisor_map
            ]
            for o in officers
        }

        def generate_entity_display_name(
            entity: Union[SupervisionOfficer, SupervisionOfficerSupervisor],
            include_email: bool = True,
        ) -> str:
            name = PersonName(**entity.full_name).formatted_first_last
            return (
                f"{name} <{entity.email or 'email not found'}>"
                if include_email
                else name
            )

        def generate_officer_supervised_by_text(o: SupervisionOfficer) -> str:
            supervisors = officer_to_supervisors.get(o, [])
            return (
                "no supervisors listed"
                if not supervisors
                else f"supervised by {', '.join(generate_entity_display_name(s, include_email=False) for s in supervisors)}"
            )

        officer_section = "".join(
            f"- {generate_entity_display_name(o)}, {o.supervision_district}{f' ({generate_officer_supervised_by_text(o)})' if change_type == RosterChangeType.ADD else ''}\n"
            for o in officers
        )
        supervisor_section = "".join(
            f"- {generate_entity_display_name(s)}\n" for s in supervisors
        )
        action = "added to" if change_type == RosterChangeType.ADD else "removed from"
        officer_label = self._get_querier_product_config.supervision_officer_label

        return (
            f"{requester_name} has requested that the following {officer_label}(s) "
            f"be {action} the caseload of {target_name}:\n"
            f"{officer_section}\n"
            f"Other supervisor(s) affected by this change:\n"
            f"{supervisor_section}\n"
            f"Note from user:\n{note}"
        )

    def request_roster_change(
        self,
        requester: str,
        email: str,
        change_type: RosterChangeType,
        target_supervisor_id: str,
        officer_ids: List[str],
        note: str,
        is_test: bool,
    ) -> RosterChangeRequestResponseSchema:
        """Creates a roster change request ticket."""
        officer_label = self._get_querier_product_config.supervision_officer_label
        officers = self._fetch_entities(
            officer_ids,
            self.querier.get_supervision_officers_by_external_ids,
            officer_label,
        )

        supervisor_ids = {target_supervisor_id} | set(
            flatten(o.supervisor_external_ids for o in officers)
        )
        supervisor_label = self._get_querier_product_config.supervision_supervisor_label
        supervisors = self._fetch_entities(
            supervisor_ids,
            self.querier.get_supervision_officer_supervisors_by_external_ids,
            supervisor_label.capitalize(),
        )

        target_supervisor = next(
            (s for s in supervisors if s.external_id == target_supervisor_id), None
        )
        # Should not happen.
        if not target_supervisor:
            raise ValueError(f"Target supervisor {target_supervisor_id} not found")

        description = self._build_ticket_description(
            requester,
            PersonName(**target_supervisor.full_name).formatted_first_last,
            change_type,
            note,
            supervisors,
            officers,
        )
        test_tag = "[TEST] " if is_test else ""
        test_disclaimer = (
            "PLEASE DISREGARD. THIS IS A TEST REQUEST.\n" if is_test else ""
        )
        action = "Addition" if change_type == RosterChangeType.ADD else "Removal"
        title = f"{test_tag}Team {action} Request Submitted"
        response = self.intercom_api_client.create_ticket(
            title, test_disclaimer + description, email
        )

        return RosterChangeRequestResponseSchema(email=email, id=response.id)
