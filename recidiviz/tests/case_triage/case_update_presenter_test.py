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
from datetime import date, datetime, timedelta
from unittest.case import TestCase

from freezegun import freeze_time

from recidiviz.case_triage.case_updates.serializers import serialize_last_version_info
from recidiviz.case_triage.case_updates.types import (
    CaseUpdateMetadataKeys,
    CaseUpdateActionType,
)
from recidiviz.case_triage.querier.case_update_presenter import (
    CaseUpdateStatus,
    CaseUpdatePresenter,
)
from recidiviz.tests.case_triage.case_triage_helpers import (
    generate_fake_client,
    generate_fake_case_update,
    generate_fake_officer,
)


class TestCaseUpdatePresenter(TestCase):
    """Implements tests for the CaseUpdatePresenter class."""

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
    def test_dismiss_actions(self) -> None:
        """This tests dismissed actions. No changes to the ETL data that we see will
        affect the values we ultimately get from this."""
        dismiss_actions = [
            CaseUpdateActionType.INFORMATION_DOESNT_MATCH_OMS,
            CaseUpdateActionType.NOT_ON_CASELOAD,
            CaseUpdateActionType.FILED_REVOCATION_OR_VIOLATION,
            CaseUpdateActionType.OTHER_DISMISSAL,
        ]

        case_updates = [
            generate_fake_case_update(
                self.mock_client,
                self.mock_officer,
                action_type=action_type,
                last_version=serialize_last_version_info(
                    action_type, self.mock_client
                ).to_json(),
            )
            for action_type in dismiss_actions
        ]

        for case_update in case_updates:
            presenter = CaseUpdatePresenter(self.mock_client, case_update)
            self.assertEqual(
                presenter.to_json()["status"], CaseUpdateStatus.IN_PROGRESS.value
            )
            self.assertEqual(
                presenter.to_json()["actionTs"], str(datetime(2020, 1, 1, 0, 0))
            )

    def test_completed_assessment_action_unresolved(self) -> None:
        presenter = CaseUpdatePresenter(
            self.mock_client,
            generate_fake_case_update(
                self.mock_client,
                self.mock_officer,
                action_type=CaseUpdateActionType.COMPLETED_ASSESSMENT,
                last_version={
                    CaseUpdateMetadataKeys.LAST_RECORDED_DATE: self.mock_client.most_recent_assessment_date.isoformat(),
                },
            ),
        )
        self.assertEqual(
            presenter.to_json()["status"], CaseUpdateStatus.IN_PROGRESS.value
        )

        # Officer filled out a new assessment for the client
        self.mock_client.most_recent_assessment_date = (
            self.mock_client.most_recent_assessment_date + timedelta(days=1)
        )

        self.assertEqual(
            presenter.to_json()["status"], CaseUpdateStatus.UPDATED_IN_CIS.value
        )

    def test_discharge_initiated_action_unresolved(self) -> None:
        presenter = CaseUpdatePresenter(
            self.mock_client,
            generate_fake_case_update(
                self.mock_client,
                self.mock_officer,
                action_type=CaseUpdateActionType.DISCHARGE_INITIATED,
            ),
        )
        self.assertEqual(
            presenter.to_json()["status"], CaseUpdateStatus.IN_PROGRESS.value
        )

    def test_downgrade_initiated_action_resolve_flow(self) -> None:
        presenter = CaseUpdatePresenter(
            self.mock_client,
            generate_fake_case_update(
                self.mock_client,
                self.mock_officer,
                action_type=CaseUpdateActionType.DOWNGRADE_INITIATED,
                last_version={
                    CaseUpdateMetadataKeys.LAST_SUPERVISION_LEVEL: self.mock_client.supervision_level,
                },
            ),
        )

        self.assertEqual(
            presenter.to_json()["status"], CaseUpdateStatus.IN_PROGRESS.value
        )

        self.mock_client.supervision_level = "MINIMUM"

        self.assertEqual(
            presenter.to_json()["status"], CaseUpdateStatus.UPDATED_IN_CIS.value
        )

    def test_found_employment_action_resolve_flow(self) -> None:
        presenter = CaseUpdatePresenter(
            self.mock_client,
            generate_fake_case_update(
                self.mock_client,
                self.mock_officer,
                action_type=CaseUpdateActionType.FOUND_EMPLOYMENT,
            ),
        )
        self.assertEqual(
            presenter.to_json()["status"], CaseUpdateStatus.IN_PROGRESS.value
        )

        # Employment updated in CIS
        self.mock_client.employer = "Recidiviz"

        self.assertEqual(
            presenter.to_json()["status"], CaseUpdateStatus.UPDATED_IN_CIS.value
        )

    def test_scheduled_face_to_face_action_resolve_flow(self) -> None:
        presenter = CaseUpdatePresenter(
            self.mock_client,
            generate_fake_case_update(
                self.mock_client,
                self.mock_officer,
                action_type=CaseUpdateActionType.SCHEDULED_FACE_TO_FACE,
                last_version={
                    CaseUpdateMetadataKeys.LAST_RECORDED_DATE: self.mock_client.most_recent_face_to_face_date.isoformat(),
                },
            ),
        )

        self.assertEqual(
            presenter.to_json()["status"], CaseUpdateStatus.IN_PROGRESS.value
        )

        # Officer had a recent face to face contact with the client
        self.mock_client.most_recent_face_to_face_date = date(2022, 2, 2)

        self.assertEqual(
            presenter.to_json()["status"], CaseUpdateStatus.UPDATED_IN_CIS.value
        )
