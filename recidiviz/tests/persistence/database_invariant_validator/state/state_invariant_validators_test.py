# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for state_database_invariant_validators.py."""

import unittest

from more_itertools import one

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema.state.dao import SessionIsDirtyError
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database_invariant_validator.database_invariant_validator import validate_invariants
from recidiviz.tests.persistence.database.schema.state.schema_test_utils import generate_external_id, generate_person
from recidiviz.tools.postgres import local_postgres_helpers

EXTERNAL_ID_1 = 'EXTERNAL_ID_1'
EXTERNAL_ID_2 = 'EXTERNAL_ID_2'
ID_TYPE_1 = 'ID_TYPE_1'
ID_TYPE_2 = 'ID_TYPE_2'


class TestStateDatabaseInvariantValidators(unittest.TestCase):
    """Tests for state_database_invariant_validators.py."""

    @classmethod
    def setUpClass(cls) -> None:
        local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        local_postgres_helpers.use_on_disk_postgresql_database(StateBase)

        self.system_level = SystemLevel.STATE
        self.state_code = 'US_XX'

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(StateBase)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database()

    def test_clean_session(self) -> None:
        # Arrange
        session = SessionFactory.for_schema_base(StateBase)

        # Act
        errors = validate_invariants(session, self.system_level, self.state_code, [])

        # Assert
        self.assertEqual(0, errors)
        session.commit()

    def test_add_person_simple_no_flush(self) -> None:
        # Arrange
        session = SessionFactory.for_schema_base(StateBase)

        db_external_id = generate_external_id(
            state_code=self.state_code,
            external_id=EXTERNAL_ID_1, id_type=ID_TYPE_1)

        db_person = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id])

        session.add(db_person)

        output_people = [db_person]

        # Act
        with self.assertRaises(SessionIsDirtyError) as e:
            _ = validate_invariants(session, self.system_level, self.state_code, output_people)

        # Assert
        self.assertEqual(str(e.exception), 'Session unexpectedly dirty - flush before querying the database.')

    def test_add_person_simple(self) -> None:
        # Arrange
        session = SessionFactory.for_schema_base(StateBase)

        db_external_id = generate_external_id(
            state_code=self.state_code,
            external_id=EXTERNAL_ID_1, id_type=ID_TYPE_1)

        db_person = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id])

        session.add(db_person)
        session.flush()

        output_people = [db_person]

        # Act
        errors = validate_invariants(session, self.system_level, self.state_code, output_people)

        # Assert
        self.assertEqual(0, errors)
        session.commit()

    def test_add_person_two_ids_same_type(self) -> None:
        # Arrange
        session = SessionFactory.for_schema_base(StateBase)

        db_external_id = generate_external_id(
            state_code=self.state_code,
            external_id=EXTERNAL_ID_1, id_type=ID_TYPE_1)

        db_external_id_2 = generate_external_id(
            state_code=self.state_code,
            external_id=EXTERNAL_ID_2, id_type=ID_TYPE_1)

        db_person = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id, db_external_id_2])

        session.add(db_person)
        session.flush()

        output_people = [db_person]

        # Act
        errors = validate_invariants(session, self.system_level, self.state_code, output_people)

        # Assert
        self.assertEqual(1, errors)
        session.rollback()

    def test_add_person_two_ids_same_type_us_pa(self) -> None:
        # Arrange
        self.state_code = 'US_PA'
        session = SessionFactory.for_schema_base(StateBase)

        db_external_id = generate_external_id(
            state_code=self.state_code,
            external_id=EXTERNAL_ID_1, id_type=ID_TYPE_1)

        db_external_id_2 = generate_external_id(
            state_code=self.state_code,
            external_id=EXTERNAL_ID_2, id_type=ID_TYPE_1)

        db_person = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id, db_external_id_2])

        session.add(db_person)
        session.flush()

        output_people = [db_person]

        # Act
        errors = validate_invariants(session, self.system_level, self.state_code, output_people)

        # Assert
        self.assertEqual(0, errors)
        session.commit()

    def test_add_person_update_with_new_id(self) -> None:
        # Arrange
        arrange_session = SessionFactory.for_schema_base(StateBase)

        db_external_id = generate_external_id(
            state_code=self.state_code,
            external_id=EXTERNAL_ID_1, id_type=ID_TYPE_1)

        db_person = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id])

        arrange_session.add(db_person)
        arrange_session.commit()

        db_external_id_2 = generate_external_id(
            state_code=self.state_code,
            external_id=EXTERNAL_ID_2, id_type=ID_TYPE_1)

        # Act
        session = SessionFactory.for_schema_base(StateBase)

        result = session.query(schema.StatePerson).all()

        person_to_update = one(result)

        person_to_update.external_ids.append(db_external_id_2)
        session.flush()

        output_people = [person_to_update]

        errors = validate_invariants(session, self.system_level, self.state_code, output_people)

        # Assert
        self.assertEqual(1, errors)
        session.rollback()

    def test_add_two_people_same_id_type(self) -> None:
        # Arrange
        db_external_id = generate_external_id(
            state_code=self.state_code,
            external_id=EXTERNAL_ID_1, id_type=ID_TYPE_1)

        db_person = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id])

        db_external_id_2 = generate_external_id(
            state_code=self.state_code,
            external_id=EXTERNAL_ID_2, id_type=ID_TYPE_1)

        db_person_2 = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id_2])

        # Act
        session = SessionFactory.for_schema_base(StateBase)

        session.add(db_person)
        session.add(db_person_2)
        session.flush()

        output_people = [db_person, db_person_2]

        errors = validate_invariants(session, self.system_level, self.state_code, output_people)

        # Assert
        self.assertEqual(0, errors)
        session.commit()
