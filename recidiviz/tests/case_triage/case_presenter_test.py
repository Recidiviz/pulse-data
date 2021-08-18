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
import json
import uuid
from datetime import date, timedelta
from unittest.case import TestCase

from freezegun import freeze_time

from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.client_info.types import PreferredContactMethod
from recidiviz.case_triage.querier.case_presenter import (
    CasePresenter,
    _json_map_dates_to_strings,
)
from recidiviz.persistence.database.schema.case_triage.schema import OfficerNote
from recidiviz.tests.case_triage.case_triage_helpers import (
    generate_fake_case_update,
    generate_fake_client,
    generate_fake_client_info,
    generate_fake_officer,
)


class TestCasePresenter(TestCase):
    """Implements tests for the CasePresenter class."""

    def setUp(self) -> None:
        self.mock_officer = generate_fake_officer("officer_id_1")
        self.mock_client = generate_fake_client(
            "person_id_1",
            supervising_officer_id=self.mock_officer.external_id,
            last_assessment_date=date(2021, 2, 1),
            last_face_to_face_date=date(2021, 1, 15),
            last_home_visit_date=date(2020, 5, 3),
        )

    @freeze_time("2020-01-01 00:00")
    def test_no_case_update_no_additional_client_info(self) -> None:
        case_presenter = CasePresenter(self.mock_client, [])

        self.assertEqual(
            case_presenter.to_json(),
            _json_map_dates_to_strings(
                {
                    "personExternalId": self.mock_client.person_external_id,
                    "fullName": json.loads(self.mock_client.full_name),
                    "gender": self.mock_client.gender,
                    "supervisingOfficerExternalId": self.mock_client.supervising_officer_external_id,
                    "currentAddress": self.mock_client.current_address,
                    "emailAddress": self.mock_client.email_address,
                    "phoneNumber": self.mock_client.phone_number,
                    "birthdate": self.mock_client.birthdate,
                    "birthdateInferredFromAge": self.mock_client.birthdate_inferred_from_age,
                    "supervisionStartDate": self.mock_client.supervision_start_date,
                    "projectedEndDate": self.mock_client.projected_end_date,
                    "supervisionType": self.mock_client.supervision_type,
                    "caseType": self.mock_client.case_type,
                    "caseUpdates": {},
                    "supervisionLevel": self.mock_client.supervision_level,
                    "stateCode": self.mock_client.state_code,
                    "employer": self.mock_client.employer,
                    "employmentStartDate": self.mock_client.employment_start_date,
                    "mostRecentAssessmentDate": self.mock_client.most_recent_assessment_date,
                    "assessmentScore": self.mock_client.assessment_score,
                    "mostRecentFaceToFaceDate": self.mock_client.most_recent_face_to_face_date,
                    "mostRecentHomeVisitDate": self.mock_client.most_recent_home_visit_date,
                    "nextAssessmentDate": date(2022, 2, 1),
                    "nextFaceToFaceDate": date(2021, 3, 1),
                    "nextHomeVisitDate": date(2021, 5, 3),
                    "needsMet": {
                        "employment": False,
                        "faceToFaceContact": True,
                        "homeVisitContact": True,
                        "assessment": True,
                    },
                    "notes": [],
                    "receivingSSIOrDisabilityIncome": False,
                    "mostRecentViolationDate": self.mock_client.most_recent_violation_date,
                    "daysWithCurrentPO": self.mock_client.days_with_current_po,
                },
            ),
        )

    @freeze_time("2020-01-01 00:00")
    def test_no_case_update(self) -> None:
        client_info = generate_fake_client_info(
            client=self.mock_client,
            preferred_name="Alex",
            preferred_contact_method=PreferredContactMethod.Text,
        )
        self.mock_client.client_info = client_info
        officer_note = OfficerNote(
            note_id=uuid.uuid4(),
            state_code=self.mock_client.state_code,
            officer_external_id=self.mock_client.supervising_officer_external_id,
            person_external_id=self.mock_client.person_external_id,
            text="Simple note!",
        )
        self.mock_client.notes = [officer_note]
        case_presenter = CasePresenter(self.mock_client, [])

        self.assertEqual(
            case_presenter.to_json(),
            _json_map_dates_to_strings(
                {
                    "personExternalId": self.mock_client.person_external_id,
                    "fullName": json.loads(self.mock_client.full_name),
                    "gender": self.mock_client.gender,
                    "supervisingOfficerExternalId": self.mock_client.supervising_officer_external_id,
                    "currentAddress": self.mock_client.current_address,
                    "emailAddress": self.mock_client.email_address,
                    "phoneNumber": self.mock_client.phone_number,
                    "birthdate": self.mock_client.birthdate,
                    "birthdateInferredFromAge": self.mock_client.birthdate_inferred_from_age,
                    "supervisionStartDate": self.mock_client.supervision_start_date,
                    "projectedEndDate": self.mock_client.projected_end_date,
                    "supervisionType": self.mock_client.supervision_type,
                    "caseType": self.mock_client.case_type,
                    "caseUpdates": {},
                    "supervisionLevel": self.mock_client.supervision_level,
                    "stateCode": self.mock_client.state_code,
                    "employer": self.mock_client.employer,
                    "employmentStartDate": self.mock_client.employment_start_date,
                    "mostRecentAssessmentDate": self.mock_client.most_recent_assessment_date,
                    "assessmentScore": self.mock_client.assessment_score,
                    "mostRecentFaceToFaceDate": self.mock_client.most_recent_face_to_face_date,
                    "mostRecentHomeVisitDate": self.mock_client.most_recent_home_visit_date,
                    "nextAssessmentDate": date(2022, 2, 1),
                    "nextFaceToFaceDate": date(2021, 3, 1),
                    "nextHomeVisitDate": date(2021, 5, 3),
                    "needsMet": {
                        "employment": False,
                        "faceToFaceContact": True,
                        "homeVisitContact": True,
                        "assessment": True,
                    },
                    "preferredName": client_info.preferred_name,
                    "preferredContactMethod": client_info.preferred_contact_method,
                    "receivingSSIOrDisabilityIncome": client_info.receiving_ssi_or_disability_income,
                    "notes": [officer_note.to_json()],
                    "mostRecentViolationDate": self.mock_client.most_recent_violation_date,
                    "daysWithCurrentPO": self.mock_client.days_with_current_po,
                },
            ),
        )

    def test_case_updates(self) -> None:
        case_presenter = CasePresenter(
            self.mock_client,
            [
                generate_fake_case_update(
                    self.mock_client,
                    self.mock_officer.external_id,
                    action_type=CaseUpdateActionType.SCHEDULED_FACE_TO_FACE,
                ),
            ],
        )

        case_json = case_presenter.to_json()
        self.assertEqual(len(case_json["caseUpdates"].keys()), 1)
        self.assertIn(
            CaseUpdateActionType.SCHEDULED_FACE_TO_FACE.value, case_json["caseUpdates"]
        )

    def test_most_recent_violation(self) -> None:
        violation_date = self.mock_client.supervision_start_date + timedelta(days=5)
        self.mock_client.most_recent_violation_date = violation_date

        case_presenter = CasePresenter(
            self.mock_client,
            [],
        )

        self.assertEqual(
            case_presenter.to_json()["mostRecentViolationDate"], str(violation_date)
        )

    def test_no_most_recent_violation(self) -> None:
        # violation predates the supervision period
        violation_date = self.mock_client.supervision_start_date - timedelta(days=5)
        self.mock_client.most_recent_violation_date = violation_date

        case_presenter = CasePresenter(
            self.mock_client,
            [],
        )

        self.assertEqual(case_presenter.to_json()["mostRecentViolationDate"], None)
