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
"""Implements presentation logic for a client's supervision-related events."""

from typing import Any, Dict, Optional

from recidiviz.case_triage.client_event.types import ClientEventType
from recidiviz.persistence.database.schema.case_triage.schema import ETLClientEvent


class ClientEventPresenter:
    """Implements presentation logic for a client-related event."""

    def __init__(self, etl_client_event: ETLClientEvent) -> None:
        self.etl_client_event = etl_client_event

    @property
    def score_metadata(self) -> Optional[Dict[str, int]]:
        try:
            diff = (
                self.etl_client_event.event_metadata["assessment_score"]
                - self.etl_client_event.event_metadata["previous_assessment_score"]
            )
        except (KeyError, ValueError):
            return None
        else:
            return {
                "score": self.etl_client_event.event_metadata["assessment_score"],
                "scoreChange": diff,
            }

    @property
    def contact_type(
        self,
    ) -> Optional[str]:
        """Optionally computes more specific contact types based on event metadata.
        Returns None if not a contact event."""
        try:
            contact_type = self.etl_client_event.event_metadata["contact_type"]
        except KeyError:
            return None
        else:
            return contact_type

    def to_json(self) -> Dict[str, Any]:
        base_dict = {
            "eventType": self.etl_client_event.event_type,
            "eventDate": self.etl_client_event.event_date.isoformat(),
        }

        if self.etl_client_event.event_type == ClientEventType.ASSESSMENT.value:
            base_dict["eventMetadata"] = self.score_metadata

        if self.etl_client_event.event_type == ClientEventType.CONTACT.value:
            base_dict["eventMetadata"] = {"contactType": self.contact_type}

        return base_dict
