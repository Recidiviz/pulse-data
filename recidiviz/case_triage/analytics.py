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
"""Contains classes used to log events to Segment."""
from base64 import b64encode
from hashlib import sha256
from typing import List

from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClient,
    ETLOfficer,
)
from recidiviz.utils.segment_client import SegmentClient


def segment_user_id_for_email(email: str) -> str:
    email_as_bytes = email.lower().encode("ascii")
    digest = sha256(email_as_bytes).digest()
    return b64encode(digest).decode("utf-8")


class CaseTriageSegmentClient(SegmentClient):
    """A Case-Triage-specific Segment client."""

    def track_person_case_updated(
        self,
        officer: ETLOfficer,
        client: ETLClient,
        previous_action_set: List[CaseUpdateActionType],
        new_action_set: List[CaseUpdateActionType],
    ) -> None:
        user_id = segment_user_id_for_email(officer.email_address)
        self.track(
            user_id,
            "backend.person_case_updated",
            {
                "personExternalId": client.person_external_id,
                "previousActionSet": [action.value for action in previous_action_set],
                "newActionSet": [action.value for action in new_action_set],
            },
        )
