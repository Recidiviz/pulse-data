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
"""Implements data models for opportunities that are not part of a database schema."""
from typing import Dict

import attr

from recidiviz.case_triage.client_utils.compliance import (
    get_assessment_due_details,
    get_contact_due_details,
    get_home_visit_due_details,
)
from recidiviz.case_triage.opportunities.types import Opportunity, OpportunityType
from recidiviz.persistence.database.schema.case_triage.schema import ETLClient

NEW_TO_CASELOAD_THRESHOLD_DAYS = 45


@attr.s(auto_attribs=True)
class ComputedOpportunity(Opportunity):
    """An opportunity not backed by an underlying database entity."""

    state_code: str
    supervising_officer_external_id: str
    person_external_id: str
    opportunity_type: str
    opportunity_metadata: dict

    @staticmethod
    def build_all_for_client(
        client: ETLClient,
    ) -> Dict[OpportunityType, "ComputedOpportunity"]:
        """Outputs all ComputedOpportunities applicable to the given client."""
        opps = {}

        client_args = {
            "state_code": client.state_code,
            "supervising_officer_external_id": client.supervising_officer_external_id,
            "person_external_id": client.person_external_id,
        }

        # employment opportunities
        if client.employer is None and not client.receiving_ssi_or_disability_income:
            opps[OpportunityType.EMPLOYMENT] = ComputedOpportunity(
                opportunity_type=OpportunityType.EMPLOYMENT.value,
                opportunity_metadata={},
                **client_args
            )

        # compliance opportunities
        assessment_due = get_assessment_due_details(client)
        if assessment_due:
            status, days_until_due = assessment_due
            opps[OpportunityType.ASSESSMENT] = ComputedOpportunity(
                opportunity_type=OpportunityType.ASSESSMENT.value,
                opportunity_metadata={
                    "status": status,
                    "daysUntilDue": days_until_due,
                },
                **client_args
            )

        contact_due = get_contact_due_details(client)
        if contact_due:
            status, days_until_due = contact_due
            opps[OpportunityType.CONTACT] = ComputedOpportunity(
                opportunity_type=OpportunityType.CONTACT.value,
                opportunity_metadata={
                    "status": status,
                    "daysUntilDue": days_until_due,
                },
                **client_args
            )

        home_visit_due = get_home_visit_due_details(client)
        if home_visit_due:
            status, days_until_due = home_visit_due
            opps[OpportunityType.HOME_VISIT] = ComputedOpportunity(
                opportunity_type=OpportunityType.HOME_VISIT.value,
                opportunity_metadata={
                    "status": status,
                    "daysUntilDue": days_until_due,
                },
                **client_args
            )

        # new client
        if (
            client.days_with_current_po is not None
            and client.days_with_current_po <= NEW_TO_CASELOAD_THRESHOLD_DAYS
        ):
            opps[OpportunityType.NEW_TO_CASELOAD] = ComputedOpportunity(
                opportunity_type=OpportunityType.NEW_TO_CASELOAD.value,
                opportunity_metadata={"daysOnCaseload": client.days_with_current_po},
                **client_args
            )

        return opps
