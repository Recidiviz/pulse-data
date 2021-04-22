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
from datetime import date, timedelta
from unittest.case import TestCase

from freezegun import freeze_time

from recidiviz.case_triage.case_updates.types import (
    CaseUpdateActionType,
)
from recidiviz.case_triage.querier.case_presenter import CasePresenter
from recidiviz.case_triage.querier.case_presenter import _json_map_dates_to_strings
from recidiviz.tests.case_triage.case_triage_helpers import (
    generate_fake_client,
    generate_fake_case_update,
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
        )
        self.mock_client.case_updates = []

    @freeze_time("2020-01-01 00:00")
    def test_no_case_update(self) -> None:
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
                    "mostRecentAssessmentDate": self.mock_client.most_recent_assessment_date,
                    "assessmentScore": self.mock_client.assessment_score,
                    "mostRecentFaceToFaceDate": self.mock_client.most_recent_face_to_face_date,
                    "nextAssessmentDate": date(2022, 2, 1),
                    "nextFaceToFaceDate": date(2021, 3, 1),
                    "needsMet": {
                        "employment": False,
                        "faceToFaceContact": True,
                        "assessment": True,
                    },
                },
                None,
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

    def test_json_mapping_offsets(self) -> None:
        base_dict = {"field": date(2022, 2, 1)}

        self.assertEqual(
            _json_map_dates_to_strings(base_dict, timedelta(days=-1))["field"],
            str(date(2022, 1, 31)),
        )
        self.assertEqual(
            _json_map_dates_to_strings(base_dict, timedelta(days=28))["field"],
            str(date(2022, 3, 1)),
        )
