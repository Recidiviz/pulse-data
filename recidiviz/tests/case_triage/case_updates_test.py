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
from typing import Optional
from unittest.case import TestCase

import pytest

from recidiviz.case_triage.case_updates import CaseUpdatesInterface, CaseUpdateActionType
from recidiviz.persistence.database.base_schema import CaseTriageBase
from recidiviz.persistence.database.schema.case_triage.schema import CaseUpdate, ETLOfficer
from recidiviz.persistence.database.session_factory import SessionFactory
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

        self.mock_officer = ETLOfficer(
            external_id='officer_id_1',
            state_code='US_ID',
            email_address='officer1@recidiviz.org',
            given_names='TEST',
            surname='OFFICER',
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
            'person_id_1',
            [CaseUpdateActionType.DISCHARGE_INITIATED],
        )

        read_session = SessionFactory.for_schema_base(CaseTriageBase)
        self.assertEqual(len(read_session.query(CaseUpdate).all()), 1)

    def test_update_case_for_person(self) -> None:
        commit_session = SessionFactory.for_schema_base(CaseTriageBase)

        CaseUpdatesInterface.update_case_for_person(
            commit_session,
            self.mock_officer,
            'person_id_1',
            [CaseUpdateActionType.DISCHARGE_INITIATED],
        )

        # Validate initial insert
        read_session = SessionFactory.for_schema_base(CaseTriageBase)
        self.assertEqual(len(read_session.query(CaseUpdate).all()), 1)
        self.assertEqual(
            read_session.query(CaseUpdate).one().update_metadata['actions'],
            [CaseUpdateActionType.DISCHARGE_INITIATED.value],
        )

        # Perform update
        commit_session = SessionFactory.for_schema_base(CaseTriageBase)

        CaseUpdatesInterface.update_case_for_person(
            commit_session,
            self.mock_officer,
            'person_id_1',
            [CaseUpdateActionType.DOWNGRADE_INITIATED],
        )

        # Validate update as expected
        read_session = SessionFactory.for_schema_base(CaseTriageBase)
        self.assertEqual(len(read_session.query(CaseUpdate).all()), 1)
        self.assertEqual(
            read_session.query(CaseUpdate).one().update_metadata['actions'],
            [CaseUpdateActionType.DOWNGRADE_INITIATED.value],
        )
