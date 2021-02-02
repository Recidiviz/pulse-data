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

from recidiviz.case_triage.querier import CaseTriageQuerier
from recidiviz.persistence.database.base_schema import CaseTriageBase
from recidiviz.persistence.database.schema.case_triage.schema import ETLClient, ETLOfficer
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tools.postgres import local_postgres_helpers


def generate_fake_officer(officer_id: str, email: str) -> ETLOfficer:
    return ETLOfficer(
        external_id=officer_id,
        email_address=email,
        state_code='US_ID',
        given_names='Test',
        surname='Officer',
    )


def generate_fake_client(client_id: str, supervising_officer_id: str) -> ETLClient:
    return ETLClient(
        person_external_id=client_id,
        full_name='TEST NAME',
        supervising_officer_external_id=supervising_officer_id,
        supervision_type='TYPE',
        case_type='TYPE',
        supervision_level='LEVEL',
        state_code='US_ID',
    )


@pytest.mark.uses_db
class TestCaseTriageQuerier(TestCase):
    """Implements tests for the CaseTriageQuerier."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        local_postgres_helpers.use_on_disk_postgresql_database(CaseTriageBase)

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(CaseTriageBase)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(cls.temp_db_dir)

    def test_fetch_officer_id_happy_path(self) -> None:
        officer_1 = generate_fake_officer('id_1', 'officer1@recidiviz.org')
        officer_2 = generate_fake_officer('id_2', 'officer2@recidiviz.org')
        session = SessionFactory.for_schema_base(CaseTriageBase)
        session.add(officer_1)
        session.add(officer_2)
        session.commit()

        read_session = SessionFactory.for_schema_base(CaseTriageBase)
        first_fetch = CaseTriageQuerier.officer_id_and_state_code_for_email(read_session, 'officer1@recidiviz.org')
        self.assertEqual(first_fetch[0], 'id_1')
        self.assertEqual(first_fetch[1], 'US_ID')
        second_fetch = CaseTriageQuerier.officer_id_and_state_code_for_email(read_session, 'OFFICER2@RECIDIVIZ.ORG')
        self.assertEqual(second_fetch[0], 'id_2')
        self.assertEqual(second_fetch[1], 'US_ID')

    def test_nonexistent_officer(self) -> None:
        session = SessionFactory.for_schema_base(CaseTriageBase)

        with self.assertRaises(sqlalchemy.orm.exc.NoResultFound):
            CaseTriageQuerier.officer_id_and_state_code_for_email(session, 'nonexistent@email.com')

        with self.assertRaises(sqlalchemy.orm.exc.NoResultFound):
            CaseTriageQuerier.clients_for_officer(session, 'nonexistent@email.com')

    def test_clients_for_officer(self) -> None:
        officer_1 = generate_fake_officer('id_1', 'officer1@recidiviz.org')
        officer_2 = generate_fake_officer('id_2', 'officer2@recidiviz.org')
        officer_3 = generate_fake_officer('id_3', 'officer3@recidiviz.org')
        client_1 = generate_fake_client('client_1', 'id_1')
        client_2 = generate_fake_client('client_2', 'id_1')
        client_3 = generate_fake_client('client_3', 'id_2')
        session = SessionFactory.for_schema_base(CaseTriageBase)
        session.add(officer_1)
        session.add(officer_2)
        session.add(officer_3)
        session.add(client_1)
        session.add(client_2)
        session.add(client_3)
        session.commit()

        read_session = SessionFactory.for_schema_base(CaseTriageBase)
        self.assertEqual(
            len(CaseTriageQuerier.clients_for_officer(read_session, 'officer1@recidiviz.org')),
            2,
        )
        self.assertEqual(
            len(CaseTriageQuerier.clients_for_officer(read_session, 'officer2@recidiviz.org')),
            1,
        )
        self.assertEqual(
            len(CaseTriageQuerier.clients_for_officer(read_session, 'officer3@recidiviz.org')),
            0,
        )
