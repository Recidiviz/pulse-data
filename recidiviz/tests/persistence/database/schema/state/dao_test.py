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
from unittest import TestCase

from recidiviz import Session
from recidiviz.common.constants.state import external_id_types
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.database.schema.state import schema
from recidiviz.tests.utils import fakes

_REGION = 'region'
_FULL_NAME = 'full_name'
_EXTERNAL_ID = 'external_id'
_EXTERNAL_ID2 = 'external_id_2'
_BIRTHDATE = datetime.date(year=2012, month=1, day=2)


class TestDao(TestCase):
    """Test that the methods in dao.py correctly read from the SQL database."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def test_readPeople_byFullName(self):
        # Arrange
        person = schema.StatePerson(person_id=8, full_name=_FULL_NAME)
        person_different_name = schema.StatePerson(person_id=9,
                                                   full_name='diff_name')

        session = Session()
        session.add(person)
        session.add(person_different_name)
        session.commit()

        # Act
        people = dao.read_people(session, full_name=_FULL_NAME, birthdate=None)

        # Assert
        expected_people = [converter.convert_schema_object_to_entity(person)]
        self.assertCountEqual(people, expected_people)

    def test_readPeople_byBirthdate(self):
        # Arrange
        person = schema.StatePerson(person_id=8, birthdate=_BIRTHDATE)
        person_different_birthdate = schema.StatePerson(
            person_id=9,
            birthdate=datetime.date(year=2002,
                                    month=1,
                                    day=2))

        session = Session()
        session.add(person)
        session.add(person_different_birthdate)
        session.commit()

        # Act
        people = dao.read_people(session, full_name=None, birthdate=_BIRTHDATE)

        # Assert
        expected_people = [converter.convert_schema_object_to_entity(person)]
        self.assertCountEqual(people, expected_people)

    def test_readPeople(self):
        # Arrange
        person = schema.StatePerson(person_id=8,
                                    full_name=_FULL_NAME,
                                    birthdate=_BIRTHDATE)
        person_different_name = schema.StatePerson(person_id=9,
                                                   full_name='diff_name')
        person_different_birthdate = schema.StatePerson(
            person_id=10,
            birthdate=datetime.date(year=2002,
                                    month=1,
                                    day=2))
        session = Session()
        session.add(person)
        session.add(person_different_name)
        session.add(person_different_birthdate)
        session.commit()

        # Act
        people = dao.read_people(session, full_name=None, birthdate=None)

        # Assert
        expected_people = [
            converter.convert_schema_object_to_entity(person),
            converter.convert_schema_object_to_entity(person_different_name),
            converter.convert_schema_object_to_entity(
                person_different_birthdate)
        ]

        self.assertCountEqual(people, expected_people)

    def test_readPeopleByExternalId(self):
        # Arrange
        person_no_match = schema.StatePerson(person_id=1)
        person_match_external_id = schema.StatePerson(person_id=2)
        person_external_id = schema.StatePersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code='us_nd',
            person=person_match_external_id,
        )
        person_match_external_id.external_ids = [person_external_id]

        session = Session()
        session.add(person_no_match)
        session.add(person_match_external_id)
        session.commit()

        ingested_person = entities.StatePerson.new_with_defaults()
        ingested_person.external_ids = \
            [entities.StatePersonExternalId.new_with_defaults(
                external_id=_EXTERNAL_ID,
                id_type=external_id_types.US_ND_SID,
                state_code='us_nd',
                person=ingested_person,
            )]

        # Act
        people = dao.read_people_by_external_ids(session, _REGION,
                                                 [ingested_person])

        # Assert
        expected_people = [
            converter.convert_schema_object_to_entity(person_match_external_id)]

        self.assertCountEqual(people, expected_people)

    def test_readPersonMultipleIdsMatch(self):
        # Arrange
        person = schema.StatePerson(person_id=1)
        person_external_id = schema.StatePersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code='us_nd',
            person=person,
        )

        person_external_id2 = schema.StatePersonExternalId(
            person_external_id_id=2,
            external_id=_EXTERNAL_ID2,
            id_type=external_id_types.US_ND_SID,
            state_code='us_nd',
            person=person,
        )
        person.external_ids = [person_external_id, person_external_id2]

        session = Session()
        session.add(person)
        session.commit()

        ingested_person = entities.StatePerson.new_with_defaults()

        ingested_person.external_ids = \
            [
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID,
                    id_type=external_id_types.US_ND_SID,
                    state_code='us_nd'),
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID2,
                    id_type=external_id_types.US_ND_SID,
                    state_code='us_nd')
            ]

        # Act
        people = dao.read_people_by_external_ids(session, _REGION,
                                                 [ingested_person])

        # Assert
        expected_people = [converter.convert_schema_object_to_entity(person)]

        self.assertCountEqual(people, expected_people)

    def test_readPersonIdsMatchMultiplePeople(self):
        # Arrange
        person1 = schema.StatePerson(person_id=1)
        person1_external_id = schema.StatePersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code='us_nd',
            person=person1,
        )
        person1.external_ids = [person1_external_id]

        person2 = schema.StatePerson(person_id=2)
        person2_external_id = schema.StatePersonExternalId(
            person_external_id_id=2,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code='us_nd',
            person=person2,
        )
        person2.external_ids = [person2_external_id]

        session = Session()
        session.add(person1)
        session.add(person2)
        session.commit()

        ingested_person = entities.StatePerson.new_with_defaults()

        ingested_person.external_ids = \
            [
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID,
                    id_type=external_id_types.US_ND_SID,
                    state_code='us_nd'),
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID2,
                    id_type=external_id_types.US_ND_SID,
                    state_code='us_nd')
            ]

        # Act
        people = dao.read_people_by_external_ids(session, _REGION,
                                                 [ingested_person])

        # Assert
        expected_people = [
            converter.convert_schema_object_to_entity(person1),
            converter.convert_schema_object_to_entity(person2)]

        self.assertCountEqual(people, expected_people)

    def test_readMultipleIngestedPeople(self):
        # Arrange
        person1 = schema.StatePerson(person_id=1)
        person1_external_id = schema.StatePersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code='us_nd',
            person=person1,
        )
        person1.external_ids = [person1_external_id]

        person2 = schema.StatePerson(person_id=2)
        person2_external_id = schema.StatePersonExternalId(
            person_external_id_id=2,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code='us_nd',
            person=person2,
        )
        person2.external_ids = [person2_external_id]

        session = Session()
        session.add(person1)
        session.add(person2)
        session.commit()

        ingested_person1 = entities.StatePerson.new_with_defaults(
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID,
                    id_type=external_id_types.US_ND_SID,
                    state_code='us_nd',
                )])

        ingested_person2 = entities.StatePerson.new_with_defaults(
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID2,
                    id_type=external_id_types.US_ND_SID,
                    state_code='us_nd',
                )])

        ingested_person3 = entities.StatePerson.new_with_defaults(
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    external_id='NONEXISTENT_ID',
                    id_type=external_id_types.US_ND_SID,
                    state_code='us_nd',
                )])

        # Act
        people = dao.read_people_by_external_ids(
            session, _REGION, [ingested_person1,
                               ingested_person2,
                               ingested_person3])

        # Assert
        expected_people = [
            converter.convert_schema_object_to_entity(person1),
            converter.convert_schema_object_to_entity(person2)]

        self.assertCountEqual(people, expected_people)

    def test_readMultipleIngestedPeopleMatchSamePerson(self):
        # Arrange
        person = schema.StatePerson(person_id=1)
        person_external_id = schema.StatePersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code='us_nd',
            person=person,
        )

        person_external_id2 = schema.StatePersonExternalId(
            person_external_id_id=2,
            external_id=_EXTERNAL_ID2,
            id_type=external_id_types.US_ND_SID,
            state_code='us_nd',
            person=person,
        )

        person.external_ids = [person_external_id, person_external_id2]

        session = Session()
        session.add(person)
        session.commit()

        ingested_person1 = entities.StatePerson.new_with_defaults(
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID,
                    id_type=external_id_types.US_ND_SID,
                    state_code='us_nd',
                )])

        ingested_person2 = entities.StatePerson.new_with_defaults(
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID2,
                    id_type=external_id_types.US_ND_SID,
                    state_code='us_nd',
                )])

        # Act
        people = dao.read_people_by_external_ids(
            session, _REGION, [ingested_person1,
                               ingested_person2])

        # Assert
        expected_people = [converter.convert_schema_object_to_entity(person)]

        self.assertCountEqual(people, expected_people)
