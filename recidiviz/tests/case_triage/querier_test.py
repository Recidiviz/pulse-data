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
"""This class implements tests for the CaseTriageQuerier."""
from typing import Optional
from unittest.case import TestCase

import pytest
import sqlalchemy.orm.exc

from recidiviz.case_triage.querier.querier import (
    CaseTriageQuerier,
    PersonDoesNotExistError,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.case_triage.case_triage_helpers import (
    generate_fake_client,
    generate_fake_officer,
)
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class TestCaseTriageQuerier(TestCase):
    """Implements tests for the CaseTriageQuerier."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_fetch_officer_id_happy_path(self) -> None:
        officer_1 = generate_fake_officer("id_1", "officer1@recidiviz.org")
        officer_2 = generate_fake_officer("id_2", "officer2@recidiviz.org")
        session = SessionFactory.for_database(self.database_key)
        session.add(officer_1)
        session.add(officer_2)
        session.commit()

        read_session = SessionFactory.for_database(self.database_key)
        first_fetch = CaseTriageQuerier.officer_for_email(
            read_session, "officer1@recidiviz.org"
        )
        self.assertEqual(first_fetch.external_id, "id_1")
        second_fetch = CaseTriageQuerier.officer_for_email(
            read_session, "OFFICER2@RECIDIVIZ.ORG"
        )
        self.assertEqual(second_fetch.external_id, "id_2")

    def test_nonexistent_officer(self) -> None:
        session = SessionFactory.for_database(self.database_key)

        with self.assertRaises(sqlalchemy.orm.exc.NoResultFound):
            CaseTriageQuerier.officer_for_email(session, "nonexistent@email.com")

    def test_clients_for_officer(self) -> None:
        officer_1 = generate_fake_officer("id_1")
        officer_2 = generate_fake_officer("id_2")
        officer_3 = generate_fake_officer("id_3")
        client_1 = generate_fake_client("client_1", supervising_officer_id="id_1")
        client_2 = generate_fake_client("client_2", supervising_officer_id="id_1")
        client_3 = generate_fake_client("client_3", supervising_officer_id="id_2")
        session = SessionFactory.for_database(self.database_key)
        session.add(officer_1)
        session.add(officer_2)
        session.add(officer_3)
        session.add(client_1)
        session.add(client_2)
        session.add(client_3)
        session.commit()

        read_session = SessionFactory.for_database(self.database_key)
        self.assertEqual(
            len(CaseTriageQuerier.clients_for_officer(read_session, officer_1)),
            2,
        )
        self.assertEqual(
            len(CaseTriageQuerier.clients_for_officer(read_session, officer_2)),
            1,
        )
        self.assertEqual(
            len(CaseTriageQuerier.clients_for_officer(read_session, officer_3)),
            0,
        )

    def test_etl_client_for_officer(self) -> None:
        officer_1 = generate_fake_officer("officer_1")
        officer_2 = generate_fake_officer("officer_2")
        client_1 = generate_fake_client(
            "client_1", supervising_officer_id=officer_1.external_id
        )
        session = SessionFactory.for_database(self.database_key)
        session.add(officer_1)
        session.add(officer_2)
        session.add(client_1)
        session.commit()

        read_session = SessionFactory.for_database(self.database_key)

        # Client does not exist at all
        with self.assertRaises(PersonDoesNotExistError):
            CaseTriageQuerier.etl_client_for_officer(
                read_session, officer_1, "nonexistent"
            )

        # Client does not exist for the officer
        with self.assertRaises(PersonDoesNotExistError):
            CaseTriageQuerier.etl_client_for_officer(
                read_session, officer_2, "client_1"
            )

        # Should not raise an error
        CaseTriageQuerier.etl_client_for_officer(read_session, officer_1, "client_1")
