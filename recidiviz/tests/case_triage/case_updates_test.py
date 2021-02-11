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
"""Implements tests for the CaseUpdatesInterface."""
from datetime import date, datetime
from typing import Optional
from unittest.case import TestCase

import pytest
from freezegun import freeze_time

from recidiviz.case_triage.case_updates.interface import CaseUpdatesInterface
from recidiviz.case_triage.case_updates.progress_checker import check_case_update_action_progress
from recidiviz.case_triage.case_updates.types import CaseUpdateAction, CaseUpdateActionType, CaseUpdateMetadataKeys
from recidiviz.persistence.database.base_schema import CaseTriageBase
from recidiviz.persistence.database.schema.case_triage.schema import CaseUpdate
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.case_triage.case_triage_helpers import generate_fake_client, generate_fake_officer
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class TestCaseUpdatesInterface(TestCase):
    """Implements tests for the CaseUpdatesInterface."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        local_postgres_helpers.use_on_disk_postgresql_database(CaseTriageBase)

        self.mock_officer = generate_fake_officer('id_1')
        self.mock_client = generate_fake_client(
            'person_id_1',
            last_assessment_date=date(2021, 2, 1),
        )

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(CaseTriageBase)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(cls.temp_db_dir)

    def test_insert_case_update_for_person(self) -> None:
        commit_session = SessionFactory.for_schema_base(CaseTriageBase)

        CaseUpdatesInterface.update_case_for_person(
            commit_session,
            self.mock_officer,
            self.mock_client,
            [CaseUpdateActionType.DISCHARGE_INITIATED],
        )

        read_session = SessionFactory.for_schema_base(CaseTriageBase)
        self.assertEqual(len(read_session.query(CaseUpdate).all()), 1)

    @freeze_time('2020-01-02 00:00')
    def test_update_case_for_person(self) -> None:
        now = datetime(2020, 1, 2, 0, 0, 0)
        commit_session = SessionFactory.for_schema_base(CaseTriageBase)

        CaseUpdatesInterface.update_case_for_person(
            commit_session,
            self.mock_officer,
            self.mock_client,
            [CaseUpdateActionType.DISCHARGE_INITIATED],
        )

        # Validate initial insert
        read_session = SessionFactory.for_schema_base(CaseTriageBase)
        self.assertEqual(len(read_session.query(CaseUpdate).all()), 1)
        self.assertEqual(
            read_session.query(CaseUpdate).one().update_metadata['actions'],
            [{
                CaseUpdateMetadataKeys.ACTION: CaseUpdateActionType.DISCHARGE_INITIATED.value,
                CaseUpdateMetadataKeys.ACTION_TIMESTAMP: str(now),
            }],
        )

        # Perform update
        commit_session = SessionFactory.for_schema_base(CaseTriageBase)

        CaseUpdatesInterface.update_case_for_person(
            commit_session,
            self.mock_officer,
            self.mock_client,
            [CaseUpdateActionType.DOWNGRADE_INITIATED],
        )

        # Validate update as expected
        read_session = SessionFactory.for_schema_base(CaseTriageBase)
        self.assertEqual(len(read_session.query(CaseUpdate).all()), 1)
        self.assertEqual(
            read_session.query(CaseUpdate).one().update_metadata['actions'],
            [{
                CaseUpdateMetadataKeys.ACTION: CaseUpdateActionType.DOWNGRADE_INITIATED.value,
                CaseUpdateMetadataKeys.ACTION_TIMESTAMP: str(now),
                CaseUpdateMetadataKeys.LAST_SUPERVISION_LEVEL: self.mock_client.supervision_level,
            }],
        )

    @freeze_time('2020-01-02 00:00')
    def test_serialize_actions(self) -> None:
        now = datetime(2020, 1, 2, 0, 0, 0)
        serialized_actions = CaseUpdatesInterface.serialize_actions(
            self.mock_client,
            list(CaseUpdateActionType),
        )

        self.assertEqual(
            serialized_actions,
            [
                {
                    'action': 'COMPLETED_ASSESSMENT',
                    'action_ts': str(now),
                    'last_recorded_date': str(self.mock_client.most_recent_assessment_date),
                },
                {
                    'action': 'DISCHARGE_INITIATED',
                    'action_ts': str(now),
                },
                {
                    'action': 'DOWNGRADE_INITIATED',
                    'action_ts': str(now),
                    'last_supervision_level': self.mock_client.supervision_level,
                },
                {
                    'action': 'FOUND_EMPLOYMENT',
                    'action_ts': str(now),
                },
                {
                    'action': 'SCHEDULED_FACE_TO_FACE',
                    'action_ts': str(now),
                },
                {
                    'action': 'INFORMATION_DOESNT_MATCH_OMS',
                    'action_ts': str(now),
                },
                {
                    'action': 'NOT_ON_CASELOAD',
                    'action_ts': str(now),
                },
                {
                    'action': 'FILED_REVOCATION_OR_VIOLATION',
                    'action_ts': str(now),
                },
                {
                    'action': 'OTHER_DISMISSAL',
                    'action_ts': str(now),
                },
            ],
        )

    def test_progress_checking(self) -> None:
        serialized_actions = CaseUpdatesInterface.serialize_actions(
            self.mock_client,
            list(CaseUpdateActionType),
        )

        for serialized_action in serialized_actions:
            self.assertTrue(
                check_case_update_action_progress(self.mock_client, CaseUpdateAction.from_json(serialized_action)),
                msg=f'Action {serialized_action["action"]} does not report in-progress == True',
            )
