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
from datetime import date
from typing import Optional
from unittest.case import TestCase

import pytest

from recidiviz.case_triage.case_updates.interface import CaseUpdatesInterface
from recidiviz.case_triage.case_updates.types import (
    CaseUpdateActionType,
    CaseUpdateMetadataKeys,
)
from recidiviz.case_triage.user_context import UserContext
from recidiviz.persistence.database.schema.case_triage.schema import CaseUpdate
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.case_triage.case_triage_helpers import (
    generate_fake_client,
    generate_fake_officer,
)
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
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        self.mock_officer = generate_fake_officer("id_1")
        self.mock_client = generate_fake_client(
            "person_id_1",
            last_assessment_date=date(2021, 2, 1),
        )
        self.mock_context = UserContext(
            email=self.mock_officer.email_address, current_user=self.mock_officer
        )

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_insert_case_update_for_person(self) -> None:
        commit_session = SessionFactory.for_database(self.database_key)

        CaseUpdatesInterface.update_case_for_person(
            commit_session,
            self.mock_context,
            self.mock_client,
            CaseUpdateActionType.DISCHARGE_INITIATED,
        )

        read_session = SessionFactory.for_database(self.database_key)
        self.assertEqual(len(read_session.query(CaseUpdate).all()), 1)

    def test_update_case_for_person(self) -> None:
        commit_session = SessionFactory.for_database(self.database_key)

        CaseUpdatesInterface.update_case_for_person(
            commit_session,
            self.mock_context,
            self.mock_client,
            CaseUpdateActionType.DOWNGRADE_INITIATED,
        )

        # Validate initial insert
        read_session = SessionFactory.for_database(self.database_key)
        self.assertEqual(len(read_session.query(CaseUpdate).all()), 1)
        case_update = read_session.query(CaseUpdate).one()
        self.assertEqual(
            case_update.action_type, CaseUpdateActionType.DOWNGRADE_INITIATED.value
        )

        # Perform update
        commit_session = SessionFactory.for_database(self.database_key)

        CaseUpdatesInterface.update_case_for_person(
            commit_session,
            self.mock_context,
            self.mock_client,
            CaseUpdateActionType.DOWNGRADE_INITIATED,
        )

        # Validate update as expected
        read_session = SessionFactory.for_database(self.database_key)
        self.assertEqual(len(read_session.query(CaseUpdate).all()), 1)
        case_update = read_session.query(CaseUpdate).one()
        self.assertEqual(
            case_update.action_type, CaseUpdateActionType.DOWNGRADE_INITIATED.value
        )
        self.assertEqual(
            case_update.last_version,
            {
                CaseUpdateMetadataKeys.LAST_SUPERVISION_LEVEL: self.mock_client.supervision_level,
            },
        )
