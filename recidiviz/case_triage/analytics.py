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
from datetime import datetime

from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.opportunities.types import (
    OpportunityDeferralType,
    OpportunityType,
)
from recidiviz.case_triage.user_context import UserContext
from recidiviz.persistence.database.schema.case_triage.schema import ETLClient
from recidiviz.utils.segment_client import SegmentClient


class CaseTriageSegmentClient(SegmentClient):
    """A Case-Triage-specific Segment client."""

    def track_opportunity_deferred(
        self,
        user_context: UserContext,
        client: ETLClient,
        opportunity: OpportunityType,
        deferred_until: datetime,
        reminder_requested: bool,
    ) -> None:
        self.track(
            user_context.segment_user_id,
            "backend.opportunity_deferred",
            {
                "personExternalId": client.person_external_id,
                "opportunity": opportunity.value,
                "deferredUntil": deferred_until,
                "reminderRequested": reminder_requested,
            },
        )

    def track_opportunity_deferral_deleted(
        self,
        user_context: UserContext,
        client: ETLClient,
        deferral_type: OpportunityDeferralType,
        deferral_id: str,
    ) -> None:
        self.track(
            user_context.segment_user_id,
            "backend.opportunity_deferral_removed",
            {
                "personExternalId": client.person_external_id,
                "deferralType": deferral_type.value,
                "deferralId": deferral_id,
            },
        )

    def track_person_action_taken(
        self,
        user_context: UserContext,
        client: ETLClient,
        action: CaseUpdateActionType,
    ) -> None:
        self.track(
            user_context.segment_user_id,
            "backend.person_action_taken",
            {
                "personExternalId": client.person_external_id,
                "actionTaken": action.value,
            },
        )

    def track_person_action_removed(
        self,
        user_context: UserContext,
        client: ETLClient,
        action: CaseUpdateActionType,
        update_id: str,
    ) -> None:
        self.track(
            user_context.segment_user_id,
            "backend.person_action_removed",
            {
                "personExternalId": client.person_external_id,
                "actionRemoved": action.value,
                "updateId": update_id,
            },
        )
