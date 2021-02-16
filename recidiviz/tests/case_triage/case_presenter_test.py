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
"""Implements tests for the CasePresenter class."""
from datetime import date, datetime
from unittest.case import TestCase

from freezegun import freeze_time

from recidiviz.case_triage.querier.case_presenter import CasePresenter
from recidiviz.case_triage.case_updates.interface import CaseUpdatesInterface
from recidiviz.case_triage.case_updates.types import CaseUpdateMetadataKeys, CaseUpdateActionType
from recidiviz.persistence.database.schema.case_triage.schema import CaseUpdate
from recidiviz.tests.case_triage.case_triage_helpers import generate_fake_client


class TestCasePresenter(TestCase):
    """Implements tests for the CasePresenter class."""

    def setUp(self) -> None:
        self.mock_client = generate_fake_client(
            'person_id_1',
            last_assessment_date=date(2021, 2, 1),
            last_face_to_face_date=date(2021, 1, 15),
        )

    @freeze_time('2020-01-01 00:00')
    def test_no_case_update(self) -> None:
        case_presenter = CasePresenter(self.mock_client, None)

        self.assertEqual(case_presenter.in_progress_officer_actions(), [])
        self.assertEqual(case_presenter.to_json(), {
            'personExternalId': self.mock_client.person_external_id,
            'fullName': self.mock_client.full_name,
            'gender': self.mock_client.gender,
            'supervisingOfficerExternalId': self.mock_client.supervising_officer_external_id,
            'currentAddress': self.mock_client.current_address,
            'birthdate': self.mock_client.birthdate,
            'birthdateInferredFromAge': self.mock_client.birthdate_inferred_from_age,
            'supervisionStartDate': self.mock_client.supervision_start_date,
            'supervisionType': self.mock_client.supervision_type,
            'caseType': self.mock_client.case_type,
            'supervisionLevel': self.mock_client.supervision_level,
            'stateCode': self.mock_client.state_code,
            'employer': self.mock_client.employer,
            'mostRecentAssessmentDate': self.mock_client.most_recent_assessment_date,
            'assessmentScore': None,
            'mostRecentFaceToFaceDate': self.mock_client.most_recent_face_to_face_date,
            'nextAssessmentDate': str(date(2022, 2, 1)),
            'nextFaceToFaceDate': str(date(2021, 3, 1)),
            'needsMet': {
                'employment': False,
                'faceToFaceContact': True,
                'assessment': True,
            },
        })

    @freeze_time('2020-01-01 00:00')
    def test_dismiss_actions(self) -> None:
        """This tests dismissed actions. No changes to the ETL data that we see will
        affect the values we ultimately get from this."""
        dismiss_actions = [
            CaseUpdateActionType.INFORMATION_DOESNT_MATCH_OMS,
            CaseUpdateActionType.NOT_ON_CASELOAD,
            CaseUpdateActionType.FILED_REVOCATION_OR_VIOLATION,
            CaseUpdateActionType.OTHER_DISMISSAL,
        ]

        case_update = CaseUpdate(
            person_external_id=self.mock_client.person_external_id,
            officer_external_id=self.mock_client.supervising_officer_external_id,
            state_code=self.mock_client.state_code,
            update_metadata={
                'actions': CaseUpdatesInterface.serialize_actions(
                    self.mock_client,
                    dismiss_actions,
                ),
            }
        )

        case_presenter = CasePresenter(self.mock_client, case_update)

        self.assertEqual(case_presenter.in_progress_officer_actions(), dismiss_actions)
        self.assertEqual(case_presenter.to_json(), {
            'personExternalId': self.mock_client.person_external_id,
            'fullName': self.mock_client.full_name,
            'gender': self.mock_client.gender,
            'supervisingOfficerExternalId': self.mock_client.supervising_officer_external_id,
            'currentAddress': self.mock_client.current_address,
            'birthdate': self.mock_client.birthdate,
            'birthdateInferredFromAge': self.mock_client.birthdate_inferred_from_age,
            'supervisionType': self.mock_client.supervision_type,
            'caseType': self.mock_client.case_type,
            'supervisionStartDate': self.mock_client.supervision_start_date,
            'supervisionLevel': self.mock_client.supervision_level,
            'stateCode': self.mock_client.state_code,
            'employer': self.mock_client.employer,
            'mostRecentAssessmentDate': self.mock_client.most_recent_assessment_date,
            'assessmentScore': None,
            'mostRecentFaceToFaceDate': self.mock_client.most_recent_face_to_face_date,
            'inProgressActions': [action.value for action in dismiss_actions],
            'nextAssessmentDate': str(date(2022, 2, 1)),
            'nextFaceToFaceDate': str(date(2021, 3, 1)),
            'needsMet': {
                'employment': False,
                'faceToFaceContact': True,
                'assessment': True,
            },
        })

    def test_completed_assessment_action_unresolved(self) -> None:
        case_update = CaseUpdate(
            person_external_id=self.mock_client.person_external_id,
            officer_external_id=self.mock_client.supervising_officer_external_id,
            state_code=self.mock_client.state_code,
            update_metadata={
                'actions': [{
                    CaseUpdateMetadataKeys.ACTION: CaseUpdateActionType.COMPLETED_ASSESSMENT.value,
                    CaseUpdateMetadataKeys.ACTION_TIMESTAMP: str(datetime.now()),
                    CaseUpdateMetadataKeys.LAST_RECORDED_DATE: self.mock_client.most_recent_assessment_date.isoformat(),
                }]
            }
        )

        case_presenter_json = CasePresenter(self.mock_client, case_update).to_json()
        self.assertEqual(case_presenter_json['inProgressActions'], [CaseUpdateActionType.COMPLETED_ASSESSMENT.value])

    def test_completed_assessment_action_resolved(self) -> None:
        case_update = CaseUpdate(
            person_external_id=self.mock_client.person_external_id,
            officer_external_id=self.mock_client.supervising_officer_external_id,
            state_code=self.mock_client.state_code,
            update_metadata={
                'actions': [{
                    CaseUpdateMetadataKeys.ACTION: CaseUpdateActionType.COMPLETED_ASSESSMENT.value,
                    CaseUpdateMetadataKeys.ACTION_TIMESTAMP: str(datetime.now()),
                    CaseUpdateMetadataKeys.LAST_RECORDED_DATE: self.mock_client.most_recent_assessment_date.isoformat(),
                }]
            }
        )

        self.mock_client.most_recent_assessment_date = date(2022, 2, 1)

        case_presenter_json = CasePresenter(self.mock_client, case_update).to_json()
        self.assertTrue('inProgressActions' not in case_presenter_json)

    def test_discharge_initiated_action_unresolved(self) -> None:
        case_update = CaseUpdate(
            person_external_id=self.mock_client.person_external_id,
            officer_external_id=self.mock_client.supervising_officer_external_id,
            state_code=self.mock_client.state_code,
            update_metadata={
                'actions': [{
                    CaseUpdateMetadataKeys.ACTION: CaseUpdateActionType.DISCHARGE_INITIATED.value,
                    CaseUpdateMetadataKeys.ACTION_TIMESTAMP: str(datetime.now()),
                }]
            }
        )

        case_presenter_json = CasePresenter(self.mock_client, case_update).to_json()
        self.assertEqual(case_presenter_json['inProgressActions'], [CaseUpdateActionType.DISCHARGE_INITIATED.value])

    def test_downgrade_initiated_action_unresolved(self) -> None:
        case_update = CaseUpdate(
            person_external_id=self.mock_client.person_external_id,
            officer_external_id=self.mock_client.supervising_officer_external_id,
            state_code=self.mock_client.state_code,
            update_metadata={
                'actions': [{
                    CaseUpdateMetadataKeys.ACTION: CaseUpdateActionType.DOWNGRADE_INITIATED.value,
                    CaseUpdateMetadataKeys.ACTION_TIMESTAMP: str(datetime.now()),
                    CaseUpdateMetadataKeys.LAST_SUPERVISION_LEVEL: self.mock_client.supervision_level,
                }]
            }
        )

        case_presenter_json = CasePresenter(self.mock_client, case_update).to_json()
        self.assertEqual(case_presenter_json['inProgressActions'], [CaseUpdateActionType.DOWNGRADE_INITIATED.value])

    def test_downgrade_initiated_action_resolved(self) -> None:
        case_update = CaseUpdate(
            person_external_id=self.mock_client.person_external_id,
            officer_external_id=self.mock_client.supervising_officer_external_id,
            state_code=self.mock_client.state_code,
            update_metadata={
                'actions': [{
                    CaseUpdateMetadataKeys.ACTION: CaseUpdateActionType.DOWNGRADE_INITIATED.value,
                    CaseUpdateMetadataKeys.ACTION_TIMESTAMP: str(datetime.now()),
                    CaseUpdateMetadataKeys.LAST_SUPERVISION_LEVEL: self.mock_client.supervision_level,
                }]
            }
        )

        self.mock_client.supervision_level = 'MINIMUM'

        case_presenter_json = CasePresenter(self.mock_client, case_update).to_json()
        self.assertTrue('inProgressActions' not in case_presenter_json)

    def test_found_employment_action_unresolved(self) -> None:
        case_update = CaseUpdate(
            person_external_id=self.mock_client.person_external_id,
            officer_external_id=self.mock_client.supervising_officer_external_id,
            state_code=self.mock_client.state_code,
            update_metadata={
                'actions': [{
                    CaseUpdateMetadataKeys.ACTION: CaseUpdateActionType.FOUND_EMPLOYMENT.value,
                    CaseUpdateMetadataKeys.ACTION_TIMESTAMP: str(datetime.now()),
                }]
            }
        )

        case_presenter_json = CasePresenter(self.mock_client, case_update).to_json()
        self.assertEqual(case_presenter_json['inProgressActions'], [CaseUpdateActionType.FOUND_EMPLOYMENT.value])

    def test_found_employment_action_resolved(self) -> None:
        case_update = CaseUpdate(
            person_external_id=self.mock_client.person_external_id,
            officer_external_id=self.mock_client.supervising_officer_external_id,
            state_code=self.mock_client.state_code,
            update_metadata={
                'actions': [{
                    CaseUpdateMetadataKeys.ACTION: CaseUpdateActionType.FOUND_EMPLOYMENT.value,
                    CaseUpdateMetadataKeys.ACTION_TIMESTAMP: str(datetime.now()),
                    CaseUpdateMetadataKeys.LAST_SUPERVISION_LEVEL: self.mock_client.supervision_level,
                }]
            }
        )

        self.mock_client.employer = 'Recidiviz'

        case_presenter_json = CasePresenter(self.mock_client, case_update).to_json()
        self.assertTrue('inProgressActions' not in case_presenter_json)

    def test_scheduled_face_to_face_action_unresolved(self) -> None:
        case_update = CaseUpdate(
            person_external_id=self.mock_client.person_external_id,
            officer_external_id=self.mock_client.supervising_officer_external_id,
            state_code=self.mock_client.state_code,
            update_metadata={
                'actions': [{
                    CaseUpdateMetadataKeys.ACTION: CaseUpdateActionType.SCHEDULED_FACE_TO_FACE.value,
                    CaseUpdateMetadataKeys.ACTION_TIMESTAMP: str(datetime.now()),
                    CaseUpdateMetadataKeys.LAST_RECORDED_DATE:
                    self.mock_client.most_recent_face_to_face_date.isoformat(),
                }]
            }
        )

        case_presenter_json = CasePresenter(self.mock_client, case_update).to_json()
        self.assertEqual(case_presenter_json['inProgressActions'], [CaseUpdateActionType.SCHEDULED_FACE_TO_FACE.value])

    def test_scheduled_face_to_face_action_resolved(self) -> None:
        case_update = CaseUpdate(
            person_external_id=self.mock_client.person_external_id,
            officer_external_id=self.mock_client.supervising_officer_external_id,
            state_code=self.mock_client.state_code,
            update_metadata={
                'actions': [{
                    CaseUpdateMetadataKeys.ACTION: CaseUpdateActionType.SCHEDULED_FACE_TO_FACE.value,
                    CaseUpdateMetadataKeys.ACTION_TIMESTAMP: str(datetime.now()),
                    CaseUpdateMetadataKeys.LAST_RECORDED_DATE:
                    self.mock_client.most_recent_face_to_face_date.isoformat(),
                }]
            }
        )

        self.mock_client.most_recent_face_to_face_date = date(2022, 2, 2)

        case_presenter_json = CasePresenter(self.mock_client, case_update).to_json()
        self.assertTrue('inProgressActions' not in case_presenter_json)
