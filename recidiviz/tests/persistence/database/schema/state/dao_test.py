# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Tests for state/dao.py."""

import datetime
from typing import Optional
from unittest import TestCase

import pytest

from recidiviz.common.constants.state import external_id_types
from recidiviz.persistence.database.schema.state import dao, schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.state import entities
from recidiviz.tools.postgres import local_postgres_helpers

_REGION = "region"
_FULL_NAME = "full_name"
_EXTERNAL_ID = "external_id"
_EXTERNAL_ID2 = "external_id_2"
_STATE_CODE = "US_ND"
_BIRTHDATE = datetime.date(year=2012, month=1, day=2)
_EMAIL = "foo@bar.gov"
_EMAIL_2 = "bar@foo.gov"


@pytest.mark.uses_db
class TestDao(TestCase):
    """Test that the methods in dao.py correctly read from the SQL database."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_readPeople(self) -> None:
        # Arrange
        person = schema.StatePerson(
            person_id=8,
            full_name=_FULL_NAME,
            birthdate=_BIRTHDATE,
            state_code=_STATE_CODE,
        )
        person_different_name = schema.StatePerson(
            person_id=9, full_name="diff_name", state_code=_STATE_CODE
        )
        person_different_birthdate = schema.StatePerson(
            state_code=_STATE_CODE,
            person_id=10,
            birthdate=datetime.date(year=2002, month=1, day=2),
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(person)
            session.add(person_different_name)
            session.add(person_different_birthdate)
            session.commit()

            # Act
            people = dao.read_all_people(session)

            # Assert
            expected_people = [
                person,
                person_different_name,
                person_different_birthdate,
            ]

            self.assertCountEqual(people, expected_people)

    def test_readPeopleByExternalId(self) -> None:
        # Arrange
        person_no_match = schema.StatePerson(person_id=1, state_code=_STATE_CODE)
        person_match_external_id = schema.StatePerson(
            person_id=2, state_code=_STATE_CODE
        )
        person_external_id = schema.StatePersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            person=person_match_external_id,
        )
        person_match_external_id.external_ids = [person_external_id]

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(person_no_match)
            session.add(person_match_external_id)
            session.commit()

            # Act
            people = dao.read_root_entities_by_external_ids(
                session, _STATE_CODE, schema.StatePerson, [_EXTERNAL_ID]
            )

            # Assert
            expected_people = [person_match_external_id]

            self.assertCountEqual(people, expected_people)

    def test_readPeopleByExternalIds_multipleExternalIdsOnlyOneMatches(self) -> None:
        # Arrange
        person = schema.StatePerson(person_id=1, state_code=_STATE_CODE)
        external_id_match = schema.StatePersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            person=person,
        )
        external_id_no_match = schema.StatePersonExternalId(
            person_external_id_id=2,
            external_id=_EXTERNAL_ID2,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            person=person,
        )
        person.external_ids = [external_id_match, external_id_no_match]

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(person)
            session.commit()

            # Act
            people = dao.read_root_entities_by_external_ids(
                session, _STATE_CODE, schema.StatePerson, [_EXTERNAL_ID]
            )

            # Assert
            expected_people = [person]

            self.assertCountEqual(people, expected_people)

    def test_readPeopleByExternalIds_MultipleIdsMatchSamePerson(self) -> None:
        # Arrange
        person = schema.StatePerson(person_id=1, state_code=_STATE_CODE)
        person_external_id = schema.StatePersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            person=person,
        )

        person_external_id2 = schema.StatePersonExternalId(
            person_external_id_id=2,
            external_id=_EXTERNAL_ID2,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            person=person,
        )
        person.external_ids = [person_external_id, person_external_id2]

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(person)
            session.commit()

            # Act
            people = dao.read_root_entities_by_external_ids(
                session, _STATE_CODE, schema.StatePerson, [_EXTERNAL_ID, _EXTERNAL_ID2]
            )

            # Assert
            expected_people = [person]

            self.assertCountEqual(people, expected_people)

    def test_readPeopleByExternalIds_IdsMatchMultiplePeople(self) -> None:
        # Arrange
        person1 = schema.StatePerson(person_id=1, state_code=_STATE_CODE)
        person1_external_id = schema.StatePersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            person=person1,
        )
        person1.external_ids = [person1_external_id]

        person2 = schema.StatePerson(person_id=2, state_code=_STATE_CODE)
        person2_external_id = schema.StatePersonExternalId(
            person_external_id_id=2,
            external_id=_EXTERNAL_ID2,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            person=person2,
        )
        person2.external_ids = [person2_external_id]

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(person1)
            session.add(person2)
            session.commit()

            ingested_person = entities.StatePerson.new_with_defaults(
                state_code=_STATE_CODE
            )

            ingested_person.external_ids = [
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID,
                    id_type=external_id_types.US_ND_SID,
                    state_code=_STATE_CODE,
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID2,
                    id_type=external_id_types.US_ND_SID,
                    state_code=_STATE_CODE,
                ),
            ]

            # Act
            people = dao.read_root_entities_by_external_ids(
                session, _STATE_CODE, schema.StatePerson, [_EXTERNAL_ID, _EXTERNAL_ID2]
            )

            # Assert
            expected_people = [person1, person2]

            self.assertCountEqual(people, expected_people)

    def test_readPeopleByExternalIds_IncludeIdWithNoMatch(self) -> None:
        # Arrange
        person1 = schema.StatePerson(person_id=1, state_code=_STATE_CODE)
        person1_external_id = schema.StatePersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            person=person1,
        )
        person1.external_ids = [person1_external_id]

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(person1)
            session.commit()

            # Act
            people = dao.read_root_entities_by_external_ids(
                session,
                _STATE_CODE,
                schema.StatePerson,
                [_EXTERNAL_ID, "NONEXISTENT_ID"],
            )

            # Assert
            expected_people = [person1]

            self.assertCountEqual(people, expected_people)

    def test_readStaff(self) -> None:
        # Arrange
        staff_member = schema.StateStaff(
            staff_id=8,
            full_name=_FULL_NAME,
            email=_EMAIL,
            state_code=_STATE_CODE,
        )
        staff_member_different_name = schema.StateStaff(
            staff_id=9, full_name="diff_name", state_code=_STATE_CODE
        )
        staff_member_different_email = schema.StateStaff(
            state_code=_STATE_CODE,
            staff_id=10,
            email=_EMAIL_2,
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(staff_member)
            session.add(staff_member_different_name)
            session.add(staff_member_different_email)
            session.commit()

            # Act
            staff = dao.read_all_staff(session)

            # Assert
            expected_staff = [
                staff_member,
                staff_member_different_name,
                staff_member_different_email,
            ]

            self.assertCountEqual(staff, expected_staff)

    def test_readStaffByExternalId(self) -> None:
        # Arrange
        staff_no_match = schema.StateStaff(staff_id=1, state_code=_STATE_CODE)
        staff_match_external_id = schema.StateStaff(staff_id=2, state_code=_STATE_CODE)
        staff_external_id = schema.StateStaffExternalId(
            staff_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            staff=staff_match_external_id,
        )
        staff_match_external_id.external_ids = [staff_external_id]

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(staff_no_match)
            session.add(staff_match_external_id)
            session.commit()

            # Act
            staff = dao.read_root_entities_by_external_ids(
                session, _STATE_CODE, schema.StateStaff, [_EXTERNAL_ID]
            )

            # Assert
            expected_staff = [staff_match_external_id]

            self.assertCountEqual(staff, expected_staff)

    def test_readStaffByExternalIds_multipleExternalIdsOnlyOneMatches(self) -> None:
        # Arrange
        staff_member = schema.StateStaff(staff_id=1, state_code=_STATE_CODE)
        external_id_match = schema.StateStaffExternalId(
            staff_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            staff=staff_member,
        )
        external_id_no_match = schema.StateStaffExternalId(
            staff_external_id_id=2,
            external_id=_EXTERNAL_ID2,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            staff=staff_member,
        )
        staff_member.external_ids = [external_id_match, external_id_no_match]

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(staff_member)
            session.commit()

            # Act
            staff = dao.read_root_entities_by_external_ids(
                session, _STATE_CODE, schema.StateStaff, [_EXTERNAL_ID]
            )

            # Assert
            expected_staff = [staff_member]

            self.assertCountEqual(staff, expected_staff)

    def test_readStaffByExternalIds_MultipleIdsMatchSameStaff(self) -> None:
        # Arrange
        staff_member = schema.StateStaff(staff_id=1, state_code=_STATE_CODE)
        staff_external_id = schema.StateStaffExternalId(
            staff_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            staff=staff_member,
        )

        staff_external_id2 = schema.StateStaffExternalId(
            staff_external_id_id=2,
            external_id=_EXTERNAL_ID2,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            staff=staff_member,
        )
        staff_member.external_ids = [staff_external_id, staff_external_id2]

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(staff_member)
            session.commit()

            # Act
            staff = dao.read_root_entities_by_external_ids(
                session, _STATE_CODE, schema.StateStaff, [_EXTERNAL_ID, _EXTERNAL_ID2]
            )

            # Assert
            expected_staff = [staff_member]

            self.assertCountEqual(staff, expected_staff)

    def test_readStaffByExternalIds_IdsMatchMultipleStaff(self) -> None:
        # Arrange
        staff1 = schema.StateStaff(staff_id=1, state_code=_STATE_CODE)
        staff1_external_id = schema.StateStaffExternalId(
            staff_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            staff=staff1,
        )
        staff1.external_ids = [staff1_external_id]

        staff2 = schema.StateStaff(staff_id=2, state_code=_STATE_CODE)
        staff2_external_id = schema.StateStaffExternalId(
            staff_external_id_id=2,
            external_id=_EXTERNAL_ID2,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            staff=staff2,
        )
        staff2.external_ids = [staff2_external_id]

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(staff1)
            session.add(staff2)
            session.commit()

            ingested_staff = entities.StateStaff.new_with_defaults(
                state_code=_STATE_CODE
            )

            ingested_staff.external_ids = [
                entities.StateStaffExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID,
                    id_type=external_id_types.US_ND_SID,
                    state_code=_STATE_CODE,
                ),
                entities.StateStaffExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID2,
                    id_type=external_id_types.US_ND_SID,
                    state_code=_STATE_CODE,
                ),
            ]

            # Act
            staff = dao.read_root_entities_by_external_ids(
                session, _STATE_CODE, schema.StateStaff, [_EXTERNAL_ID, _EXTERNAL_ID2]
            )

            # Assert
            expected_staff = [staff1, staff2]

            self.assertCountEqual(staff, expected_staff)

    def test_readStaffByExternalIds_IncludeIdWithNoMatch(self) -> None:
        # Arrange
        staff1 = schema.StateStaff(staff_id=1, state_code=_STATE_CODE)
        staff1_external_id = schema.StateStaffExternalId(
            staff_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            staff=staff1,
        )
        staff1.external_ids = [staff1_external_id]

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(staff1)
            session.commit()

            # Act
            staff = dao.read_root_entities_by_external_ids(
                session,
                _STATE_CODE,
                schema.StateStaff,
                [_EXTERNAL_ID, "NONEXISTENT_ID"],
            )

            # Assert
            expected_staff = [staff1]

            self.assertCountEqual(staff, expected_staff)

    def test_readRootEntitiesByExternalIds_InvalidRootEntityType(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            with self.assertRaisesRegex(
                ValueError, r"Unexpected root entity cls \[StateSupervisionPeriod\]"
            ):
                _ = dao.read_root_entities_by_external_ids(
                    session,
                    _STATE_CODE,
                    schema.StateSupervisionPeriod,  # type: ignore[arg-type]
                    [_EXTERNAL_ID],
                )

    def test_bothPeopleAndStaffPersisted(self) -> None:
        """Tests that when StateStaff and StatePerson objects are persisted with the
        exact same primary key / external ID info, we return the appropriate objects
        for the appropriate method calls.
        """

        # Arrange
        staff_member = schema.StateStaff(staff_id=1, state_code=_STATE_CODE)
        staff_external_id = schema.StateStaffExternalId(
            staff_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            staff=staff_member,
        )
        staff_member.external_ids = [staff_external_id]

        person = schema.StatePerson(person_id=1, state_code=_STATE_CODE)
        person_external_id = schema.StatePersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            person=person,
        )
        person.external_ids = [person_external_id]

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(staff_member)
            session.add(person)
            session.commit()

            # Act
            staff = dao.read_root_entities_by_external_ids(
                session, _STATE_CODE, schema.StateStaff, [_EXTERNAL_ID]
            )
            people = dao.read_root_entities_by_external_ids(
                session, _STATE_CODE, schema.StatePerson, [_EXTERNAL_ID]
            )

            # Assert
            self.assertCountEqual([staff_member], staff)
            self.assertCountEqual([person], people)

            # Act
            staff = dao.read_all_staff(session)
            people = dao.read_all_people(session)

            # Assert
            self.assertCountEqual([staff_member], staff)
            self.assertCountEqual([person], people)
