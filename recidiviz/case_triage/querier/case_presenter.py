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
"""Implements a CasePresenter abstraction which reconciles knowledge about
clients from our ETL pipeline with information received from POs on actions
taken to give us a unified view of a person on supervision."""
import json
from datetime import date
from typing import Any, Dict, List

from recidiviz.case_triage.client_utils.compliance import get_next_home_visit_date
from recidiviz.case_triage.demo_helpers import unconvert_fake_person_id_for_demo_user
from recidiviz.case_triage.querier.case_update_presenter import CaseUpdatePresenter
from recidiviz.case_triage.querier.utils import _json_map_dates_to_strings
from recidiviz.persistence.database.schema.case_triage.schema import (
    CaseUpdate,
    ETLClient,
)


class CasePresenter:
    """Implements the case presenter abstraction."""

    def __init__(
        self,
        etl_client: ETLClient,
        case_updates: List[CaseUpdate],
    ):
        self.etl_client = etl_client
        self.case_updates = case_updates

    def to_json(self) -> Dict[str, Any]:
        """Converts QueriedClient to json representation for frontend."""

        try:
            parsed_name = json.loads(self.etl_client.full_name)
        except (json.JSONDecodeError, TypeError):
            parsed_name = None

        base_dict = {
            "personExternalId": unconvert_fake_person_id_for_demo_user(
                self.etl_client.person_external_id
            ),
            "fullName": parsed_name,
            "gender": self.etl_client.gender,
            "supervisingOfficerExternalId": self.etl_client.supervising_officer_external_id,
            "currentAddress": self.etl_client.current_address,
            "birthdate": self.etl_client.birthdate,
            "supervisionType": self.etl_client.supervision_type,
            "caseType": self.etl_client.case_type,
            "supervisionStartDate": self.etl_client.supervision_start_date,
            "projectedEndDate": self.etl_client.projected_end_date,
            "supervisionLevel": self.etl_client.supervision_level,
            "stateCode": self.etl_client.state_code,
            "employer": self.etl_client.employer,
            "employmentStartDate": self.etl_client.employment_start_date,
            "mostRecentAssessmentDate": self.etl_client.most_recent_assessment_date,
            "assessmentScore": self.etl_client.assessment_score,
            "nextAssessmentDate": self.etl_client.next_recommended_assessment_date,
            "mostRecentFaceToFaceDate": self.etl_client.most_recent_face_to_face_date,
            "nextFaceToFaceDate": self.etl_client.next_recommended_face_to_face_date,
            "mostRecentHomeVisitDate": self.etl_client.most_recent_home_visit_date,
            "emailAddress": self.etl_client.email_address,
            "phoneNumber": self.etl_client.phone_number,
            "receivingSSIOrDisabilityIncome": self.etl_client.receiving_ssi_or_disability_income,
            "caseUpdates": {
                case_update.action_type: CaseUpdatePresenter(
                    self.etl_client, case_update
                ).to_json()
                for case_update in self.case_updates
            },
            "notes": [note.to_json() for note in self.etl_client.notes],
            "daysWithCurrentPO": self.etl_client.days_with_current_po,
        }

        # TODO(#5768): Eventually move next home visit date to our calculate pipeline.
        next_home_visit_date = get_next_home_visit_date(self.etl_client)
        if next_home_visit_date:
            base_dict["nextHomeVisitDate"] = next_home_visit_date

        today = date.today()
        base_dict["needsMet"] = {
            # Sometimes the employer field has "UNEMP" in it somewher to indicate unemployment
            "employment": bool(self.etl_client.employer)
            and "UNEMP" not in self.etl_client.employer.upper(),
            # If the F2F contact is missing, that means there may be no need for it. Otherwise, if the
            # next due face to face contact is after today, the need is met.
            "faceToFaceContact": self.etl_client.next_recommended_face_to_face_date
            is None
            or self.etl_client.next_recommended_face_to_face_date > today,
            # If the home visit contact is missing, that means there may be no need for it. Otherwise, if the
            # next due home visit contact is after today, the need is met.
            # TODO(#7320): This field determines whether compliance is up-to-date assuming
            # the rules apply to F2F home visits and not collateral home visits. More work is
            # needed to determine what the correct application of rules should actually be.
            "homeVisitContact": next_home_visit_date is None
            or next_home_visit_date > today,
            # If the next assessment is due after today, the need is met.
            "assessment": self.etl_client.next_recommended_assessment_date is None
            or self.etl_client.next_recommended_assessment_date > today,
        }

        if (client_info := self.etl_client.client_info) is not None:
            if client_info.preferred_name is not None:
                base_dict["preferredName"] = client_info.preferred_name
            if client_info.preferred_contact_method is not None:
                base_dict[
                    "preferredContactMethod"
                ] = client_info.preferred_contact_method

        # ignore any most recent violation if it predates the current supervision period
        most_recent_violation_date = self.etl_client.most_recent_violation_date
        supervision_start_date = self.etl_client.supervision_start_date
        if (
            most_recent_violation_date is None
            or supervision_start_date is None
            or most_recent_violation_date < supervision_start_date
        ):
            most_recent_violation_date = None
        base_dict["mostRecentViolationDate"] = most_recent_violation_date

        return _json_map_dates_to_strings(base_dict)
