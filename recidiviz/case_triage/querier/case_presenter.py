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
import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import numpy as np

# TODO(#5768): Remove some of these imports once we've figured out our preferred contact method.
# TODO(#5769): Remove the rest of these imports when we've moved nextAssessmentDate to the calc pipeline.
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_compliance import (
    NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS,
    NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS,
    NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS,
    REASSESSMENT_DEADLINE_DAYS,
    SEX_OFFENSE_LSIR_MINIMUM_SCORE,
    SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS,
    US_ID_SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS,
)
from recidiviz.case_triage.demo_helpers import unconvert_fake_person_id_for_demo_user
from recidiviz.case_triage.querier.case_update_presenter import CaseUpdatePresenter
from recidiviz.case_triage.querier.utils import _json_map_dates_to_strings
from recidiviz.common.constants.person_characteristics import Gender
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)
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

    def to_json(self, timedelta_shift: Optional[timedelta] = None) -> Dict[str, Any]:
        """Converts QueriedClient to json representation for frontend.

        If the `timedelta_shift` parameter is provided, this json-based representation
        will shift its values by the given timedelta. This is primarily used in demo
        mode.
        """

        try:
            parsed_name = json.loads(self.etl_client.full_name)
        except json.JSONDecodeError:
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
            "birthdateInferredFromAge": self.etl_client.birthdate_inferred_from_age,
            "supervisionType": self.etl_client.supervision_type,
            "caseType": self.etl_client.case_type,
            "supervisionStartDate": self.etl_client.supervision_start_date,
            "projectedEndDate": self.etl_client.projected_end_date,
            "supervisionLevel": self.etl_client.supervision_level,
            "stateCode": self.etl_client.state_code,
            "employer": self.etl_client.employer,
            "mostRecentAssessmentDate": self.etl_client.most_recent_assessment_date,
            "assessmentScore": self.etl_client.assessment_score,
            "mostRecentFaceToFaceDate": self.etl_client.most_recent_face_to_face_date,
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
        }

        # TODO(#5769): We're doing this quickly here and being intentional about the debt we're taking
        # on. This will be moved to the calculation pipeline once we've shipped the Case Triage MVP.
        next_assessment_date = self._next_assessment_date()
        base_dict["nextAssessmentDate"] = next_assessment_date

        # TODO(#5768): In the long-term, we plan to move away from enforcing the next contact
        # and next assessment so explicitly. This is why we're implementing this in QueriedClient
        # and hard-coding the relation to US_ID as a quick stop gap, as opposed to putting this in
        # the calculation pipeline where this information _should_ reside.
        next_face_to_face_date = self._next_face_to_face_date()
        if next_face_to_face_date:
            base_dict["nextFaceToFaceDate"] = next_face_to_face_date
        next_home_visit_date = self._next_home_visit_date()
        if next_home_visit_date:
            base_dict["nextHomeVisitDate"] = next_home_visit_date

        today = date.today()
        base_dict["needsMet"] = {
            # Sometimes the employer field has "UNEMP" in it somewher to indicate unemployment
            "employment": bool(self.etl_client.employer)
            and "UNEMP" not in self.etl_client.employer.upper(),
            # If the F2F contact is missing, that means there may be no need for it. Otherwise, if the
            # next due face to face contact is after today, the need is met.
            "faceToFaceContact": next_face_to_face_date is None
            or bool(next_face_to_face_date > today),
            # If the home visit contact is missing, that means there may be no need for it. Otherwise, if the
            # next due home visit contact is after today, the need is met.
            # TODO(#7320): This field determines whether compliance is up-to-date assuming
            # the rules apply to F2F home visits and not collateral home visits. More work is
            # needed to determine what the correct application of rules should actually be.
            "homeVisitContact": next_home_visit_date is None
            or bool(next_home_visit_date > today),
            # If the next assessment is due after today, the need is met.
            "assessment": next_assessment_date is None
            or bool(next_assessment_date > today),
        }

        if (client_info := self.etl_client.client_info) is not None:
            if client_info.preferred_name is not None:
                base_dict["preferredName"] = client_info.preferred_name
            if client_info.preferred_contact_method is not None:
                base_dict[
                    "preferredContactMethod"
                ] = client_info.preferred_contact_method

        return _json_map_dates_to_strings(base_dict, timedelta_shift)

    def _next_assessment_date(self) -> Optional[date]:
        """Calculates the next assessment date for the given case.

        TODO(#5769): Eventually move this method to our calculate pipeline.
        """
        if (
            self.etl_client.most_recent_assessment_date is None
            or self.etl_client.assessment_score is None
        ):
            if self.etl_client.supervision_start_date is None:
                # We expect that supervision_start_date is filled in, but in instances where
                # our default calc pipeline look back period is shorter than the amount of time
                # someone has been on supervision, it will be empty.
                #
                # We log the warning, but still want to fail gracefully.
                logging.warning(
                    "Supervision start date unexpectedly empty for client with id %s in state %s",
                    self.etl_client.person_external_id,
                    self.etl_client.state_code,
                )
                return None
            return self.etl_client.supervision_start_date + timedelta(
                days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS
            )
        if self.etl_client.case_type == StateSupervisionCaseType.SEX_OFFENSE.value:
            if self.etl_client.assessment_score < SEX_OFFENSE_LSIR_MINIMUM_SCORE.get(
                Gender(self.etl_client.gender), 0
            ):
                return None
        if (
            StateSupervisionLevel(self.etl_client.supervision_level)
            == StateSupervisionLevel.MINIMUM
        ):
            return None
        return self.etl_client.most_recent_assessment_date + timedelta(
            days=REASSESSMENT_DEADLINE_DAYS
        )

    def _next_face_to_face_date(self) -> Optional[date]:
        """This method returns the next face-to-face contact date. It returns None if no
        future face-to-face contact is required."""
        # TODO(#5768): Eventually delete or move this method to our calculate pipeline.
        if self.etl_client.most_recent_face_to_face_date is None:
            return np.busday_offset(
                self.etl_client.supervision_start_date,
                NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS,
                roll="forward",
            ).astype(date)

        case_type = StateSupervisionCaseType(self.etl_client.case_type)
        supervision_level = StateSupervisionLevel(self.etl_client.supervision_level)
        if (
            case_type not in SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS
            or supervision_level
            not in SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[case_type]
        ):
            logging.warning(
                "Could not find requirements for case type %s, supervision level %s",
                self.etl_client.case_type,
                self.etl_client.supervision_level,
            )
            return None

        face_to_face_requirements = SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[
            case_type
        ][supervision_level]
        if face_to_face_requirements[0] == 0:
            return None
        return self.etl_client.most_recent_face_to_face_date + timedelta(
            days=(face_to_face_requirements[1] // face_to_face_requirements[0])
        )

    def _next_home_visit_date(self) -> Optional[date]:
        """This method returns the next home visit contact date. It returns None if no
        future home visit contact is required."""
        # TODO(#5768): Eventually delete or move this method to our calculate pipeline.
        if self.etl_client.most_recent_home_visit_date is None:
            return self.etl_client.supervision_start_date + timedelta(
                days=NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS
            )

        supervision_level = StateSupervisionLevel(self.etl_client.supervision_level)
        if supervision_level not in US_ID_SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS:
            logging.warning(
                "Could not find requirements for supervision level %s",
                self.etl_client.supervision_level,
            )
            return None

        home_visit_requirements = US_ID_SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS[
            supervision_level
        ]
        if home_visit_requirements[0] == 0:
            return None
        return self.etl_client.most_recent_home_visit_date + timedelta(
            days=(home_visit_requirements[1] // home_visit_requirements[0])
        )
