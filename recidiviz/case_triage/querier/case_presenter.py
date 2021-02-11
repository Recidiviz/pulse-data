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
from typing import Any, Dict, List, Optional

from recidiviz.case_triage.case_updates.progress_checker import check_case_update_action_progress
from recidiviz.case_triage.case_updates.types import CaseUpdateAction, CaseUpdateActionType
from recidiviz.persistence.database.schema.case_triage.schema import CaseUpdate, ETLClient


class CasePresenter:
    """Implements the case presenter abstraction."""

    def __init__(self, etl_client: ETLClient, case_update: Optional[CaseUpdate]):
        self.etl_client = etl_client
        self.case_update = case_update

    def to_json(self) -> Dict[str, Any]:
        base_dict = {
            'personExternalId': self.etl_client.person_external_id,
            'fullName': self.etl_client.full_name,
            'supervisingOfficerExternalId': self.etl_client.supervising_officer_external_id,
            'currentAddress': self.etl_client.current_address,
            'birthdate': self.etl_client.birthdate,
            'birthdateInferredFromAge': self.etl_client.birthdate_inferred_from_age,
            'supervisionType': self.etl_client.supervision_type,
            'caseType': self.etl_client.case_type,
            'supervisionLevel': self.etl_client.supervision_level,
            'stateCode': self.etl_client.state_code,
            'employer': self.etl_client.employer,
            'mostRecentAssessmentDate': self.etl_client.most_recent_assessment_date,
            'mostRecentFaceToFaceDate': self.etl_client.most_recent_face_to_face_date,
        }

        in_progress_actions = self.in_progress_officer_actions()
        if in_progress_actions:
            base_dict['inProgressActions'] = [action.value for action in in_progress_actions]

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
