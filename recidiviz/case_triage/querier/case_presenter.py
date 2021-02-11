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
import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import numpy as np

# TODO(#5768): Remove some of these imports once we've figured out our preferred contact method.
# TODO(#5769): Remove the rest of these imports when we've moved nextAssessmentDate to the calc pipeline.
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_compliance import (
    NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS,
    NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS,
    REASSESSMENT_DEADLINE_DAYS,
    SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS,
)
from recidiviz.case_triage.case_updates.progress_checker import check_case_update_action_progress
from recidiviz.case_triage.case_updates.types import CaseUpdateAction, CaseUpdateActionType
from recidiviz.persistence.database.schema.case_triage.schema import CaseUpdate, ETLClient


class CasePresenter:
    """Implements the case presenter abstraction."""

    def __init__(self, etl_client: ETLClient, case_update: Optional[CaseUpdate]):
        self.etl_client = etl_client
        self.case_update = case_update

    def to_json(self) -> Dict[str, Any]:
        """Converts QueriedClient to json representation for frontend."""
        base_dict = {
            'personExternalId': self.etl_client.person_external_id,
            'fullName': self.etl_client.full_name,
            'gender': self.etl_client.gender,
            'supervisingOfficerExternalId': self.etl_client.supervising_officer_external_id,
            'currentAddress': self.etl_client.current_address,
            'birthdate': self.etl_client.birthdate,
            'birthdateInferredFromAge': self.etl_client.birthdate_inferred_from_age,
            'supervisionType': self.etl_client.supervision_type,
            'caseType': self.etl_client.case_type,
            'supervisionStartDate': self.etl_client.supervision_start_date,
            'supervisionLevel': self.etl_client.supervision_level,
            'stateCode': self.etl_client.state_code,
            'employer': self.etl_client.employer,
            'mostRecentAssessmentDate': self.etl_client.most_recent_assessment_date,
            'mostRecentFaceToFaceDate': self.etl_client.most_recent_face_to_face_date,
        }

        in_progress_actions = self.in_progress_officer_actions()
        if in_progress_actions:
            base_dict['inProgressActions'] = [action.value for action in in_progress_actions]

        # TODO(#5769): We're doing this quickly here and being intentional about the debt we're taking
        # on. This will be moved to the calculation pipeline once we've shipped the Case Triage MVP.
        base_dict['nextAssessmentDate'] = str(self._next_assessment_date())

        # TODO(#5768): In the long-term, we plan to move away from enforcing the next contact
        # and next assessment so explicitly. This is why we're implementing this in QueriedClient
        # and hard-coding the relation to US_ID as a quick stop gap, as opposed to putting this in
        # the calculation pipeline where this information _should_ reside.
        next_face_to_face_date = self._next_face_to_face_date()
        if next_face_to_face_date:
            base_dict['nextFaceToFaceDate'] = str(next_face_to_face_date)

        return base_dict

    def in_progress_officer_actions(self) -> List[CaseUpdateActionType]:
        """Calculates the list of CaseUpdateActionTypes that are still applicable for the client."""
        if not self.case_update:
            return []

        action_info: List[Dict[str, Any]] = self.case_update.update_metadata['actions']
        in_progress_actions: List[CaseUpdateActionType] = []
        for action_metadata in action_info:
            try:
                case_update_action = CaseUpdateAction.from_json(action_metadata)
            except ValueError:
                logging.warning('Unknown CaseUpdateAction found in update_metadata %s', action_metadata)
                continue

            if check_case_update_action_progress(self.etl_client, case_update_action):
                in_progress_actions.append(case_update_action.action_type)

        return in_progress_actions

    def _next_assessment_date(self) -> date:
        # TODO(#5769): Eventually move this method to our calculate pipeline.
        if self.etl_client.most_recent_assessment_date is None:
            return self.etl_client.supervision_start_date + timedelta(days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS)
        return self.etl_client.most_recent_assessment_date + timedelta(days=REASSESSMENT_DEADLINE_DAYS)

    def _next_face_to_face_date(self) -> Optional[date]:
        """This method returns the next face-to-face contact date. It returns None if no
        future face-to-face contact is required."""
        # TODO(#5768): Eventually delete or move this method to our calculate pipeline.
        if self.etl_client.most_recent_face_to_face_date is None:
            return np.busday_offset(
                self.etl_client.supervision_start_date,
                NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS,
                roll='forward',
            )

        case_type = self.etl_client.case_type
        supervision_level = self.etl_client.supervision_level
        if case_type not in SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS or \
                supervision_level not in SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[case_type]:
            logging.warning(
                'Could not find requirements for case type %s, supervision level %s',
                self.etl_client.case_type,
                self.etl_client.supervision_level,
            )
            return None

        face_to_face_requirements = SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[case_type][supervision_level]
        if face_to_face_requirements[0] == 0:
            return None
        return self.etl_client.most_recent_face_to_face_date + timedelta(
            days=(face_to_face_requirements[1] // face_to_face_requirements[0])
        )
