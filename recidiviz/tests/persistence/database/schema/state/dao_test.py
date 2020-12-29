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
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.database.schema.state import schema
from recidiviz.tools.postgres import local_postgres_helpers

_REGION = 'region'
_FULL_NAME = 'full_name'
_EXTERNAL_ID = 'external_id'
_EXTERNAL_ID2 = 'external_id_2'
_STATE_CODE = 'US_ND'
_BIRTHDATE = datetime.date(year=2012, month=1, day=2)


@pytest.mark.uses_db
class TestDao(TestCase):
    """Test that the methods in dao.py correctly read from the SQL database."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        local_postgres_helpers.use_on_disk_postgresql_database(StateBase)

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(StateBase)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(cls.temp_db_dir)

    def test_readPeople_byFullName(self) -> None:
        # Arrange
        person = schema.StatePerson(person_id=8,
                                    full_name=_FULL_NAME,
                                    state_code=_STATE_CODE)
        person_different_name = schema.StatePerson(person_id=9,
                                                   full_name='diff_name',
                                                   state_code=_STATE_CODE)

        session = SessionFactory.for_schema_base(StateBase)
        session.add(person)
        session.add(person_different_name)
        session.commit()

        # Act
        people = dao.read_people(session, full_name=_FULL_NAME, birthdate=None)

        # Assert
        expected_people = [person]
        self.assertCountEqual(people, expected_people)

    def test_readPeople_byBirthdate(self) -> None:
        # Arrange
        person = schema.StatePerson(person_id=8,
                                    birthdate=_BIRTHDATE,
                                    state_code=_STATE_CODE)
        person_different_birthdate = schema.StatePerson(
            state_code=_STATE_CODE,
            person_id=9,
            birthdate=datetime.date(year=2002,
                                    month=1,
                                    day=2))

        session = SessionFactory.for_schema_base(StateBase)
        session.add(person)
        session.add(person_different_birthdate)
        session.commit()

        # Act
        people = dao.read_people(session, full_name=None, birthdate=_BIRTHDATE)

        # Assert
        expected_people = [person]
        self.assertCountEqual(people, expected_people)

    def test_readPeople(self) -> None:
        # Arrange
        person = schema.StatePerson(person_id=8,
                                    full_name=_FULL_NAME,
                                    birthdate=_BIRTHDATE,
                                    state_code=_STATE_CODE)
        person_different_name = schema.StatePerson(person_id=9,
                                                   full_name='diff_name',
                                                   state_code=_STATE_CODE)
        person_different_birthdate = schema.StatePerson(
            state_code=_STATE_CODE,
            person_id=10,
            birthdate=datetime.date(year=2002,
                                    month=1,
                                    day=2))
        session = SessionFactory.for_schema_base(StateBase)
        session.add(person)
        session.add(person_different_name)
        session.add(person_different_birthdate)
        session.commit()

        # Act
        people = dao.read_people(session, full_name=None, birthdate=None)

        # Assert
        expected_people = [
            person,
            person_different_name,
            person_different_birthdate
        ]

        self.assertCountEqual(people, expected_people)

    def test_readPlaceholderPeople(self) -> None:
        placeholder_person = schema.StatePerson(person_id=1, state_code=_STATE_CODE)
        person = schema.StatePerson(person_id=2, state_code=_STATE_CODE)
        person_external_id = schema.StatePersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            person=person,
        )
        person.external_ids = [person_external_id]

        session = SessionFactory.for_schema_base(StateBase)
        session.add(placeholder_person)
        session.add(person)
        session.commit()

        # Act
        people = dao.read_placeholder_persons(session, _STATE_CODE)

        # Assert
        expected_people = [placeholder_person]

        self.assertCountEqual(people, expected_people)

    def test_readPeopleByRootExternalIds(self) -> None:
        # Arrange
        person_no_match = schema.StatePerson(person_id=1, state_code=_STATE_CODE)
        person_match_external_id = schema.StatePerson(person_id=2, state_code=_STATE_CODE)
        person_external_id = schema.StatePersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            person=person_match_external_id,
        )
        person_match_external_id.external_ids = [person_external_id]

        session = SessionFactory.for_schema_base(StateBase)
        session.add(person_no_match)
        session.add(person_match_external_id)
        session.commit()

        # Act
        people = dao.read_people_by_cls_external_ids(
            session, _STATE_CODE, schema.StatePerson, [_EXTERNAL_ID])

        # Assert
        expected_people = [person_match_external_id]

        self.assertCountEqual(people, expected_people)

    def test_readPeopleByRootExternalIds_entireTreeReturnedWithOneMatch(self) -> None:
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

        session = SessionFactory.for_schema_base(StateBase)
        session.add(person)
        session.commit()

        # Act
        people = dao.read_people_by_cls_external_ids(
            session, _STATE_CODE, schema.StatePerson, [_EXTERNAL_ID])

        # Assert
        expected_people = [person]

        self.assertCountEqual(people, expected_people)

    def test_readPeopleByRootExternalIds_SentenceGroupExternalId(self) -> None:
        # Arrange
        person = schema.StatePerson(person_id=1, state_code=_STATE_CODE)
        sentence_group = schema.StateSentenceGroup(
            sentence_group_id=1,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            person=person)
        sentence_group_2 = schema.StateSentenceGroup(
            sentence_group_id=2,
            external_id=_EXTERNAL_ID2,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            person=person)
        person.sentence_groups = [sentence_group, sentence_group_2]

        session = SessionFactory.for_schema_base(StateBase)
        session.add(person)
        session.commit()

        # Act
        people = dao.read_people_by_cls_external_ids(
            session, _STATE_CODE, schema.StateSentenceGroup, [_EXTERNAL_ID])

        # Assert
        expected_people = [person]

        self.assertCountEqual(people, expected_people)

    def test_readPeopleByExternalId(self) -> None:
        # Arrange
        person_no_match = schema.StatePerson(person_id=1, state_code=_STATE_CODE)
        person_match_external_id = schema.StatePerson(person_id=2, state_code=_STATE_CODE)
        person_external_id = schema.StatePersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            id_type=external_id_types.US_ND_SID,
            state_code=_STATE_CODE,
            person=person_match_external_id,
        )
        person_match_external_id.external_ids = [person_external_id]

        session = SessionFactory.for_schema_base(StateBase)
        session.add(person_no_match)
        session.add(person_match_external_id)
        session.commit()

        ingested_person = entities.StatePerson.new_with_defaults(state_code=_STATE_CODE)
        ingested_person.external_ids = \
            [entities.StatePersonExternalId.new_with_defaults(
                external_id=_EXTERNAL_ID,
                id_type=external_id_types.US_ND_SID,
                state_code=_STATE_CODE,
                person=ingested_person,
            )]

        # Act
        people = dao.read_people_by_external_ids(session, _REGION,
                                                 [ingested_person])

        # Assert
        expected_people = [person_match_external_id]

        self.assertCountEqual(people, expected_people)

    def test_readPersonMultipleIdsMatch(self) -> None:
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

        session = SessionFactory.for_schema_base(StateBase)
        session.add(person)
        session.commit()

        ingested_person = entities.StatePerson.new_with_defaults(state_code=_STATE_CODE)

        ingested_person.external_ids = \
            [
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID,
                    id_type=external_id_types.US_ND_SID,
                    state_code=_STATE_CODE),
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID2,
                    id_type=external_id_types.US_ND_SID,
                    state_code=_STATE_CODE)
            ]

        # Act
        people = dao.read_people_by_external_ids(session, _REGION,
                                                 [ingested_person])

        # Assert
        expected_people = [person]

        self.assertCountEqual(people, expected_people)

    def test_readPersonIdsMatchMultiplePeople(self) -> None:
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

        session = SessionFactory.for_schema_base(StateBase)
        session.add(person1)
        session.add(person2)
        session.commit()

        ingested_person = entities.StatePerson.new_with_defaults(state_code=_STATE_CODE)

        ingested_person.external_ids = \
            [
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID,
                    id_type=external_id_types.US_ND_SID,
                    state_code=_STATE_CODE),
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID2,
                    id_type=external_id_types.US_ND_SID,
                    state_code=_STATE_CODE)
            ]

        # Act
        people = dao.read_people_by_external_ids(session, _REGION,
                                                 [ingested_person])

        # Assert
        expected_people = [person1, person2]

        self.assertCountEqual(people, expected_people)

    def test_readMultipleIngestedPeople(self) -> None:
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

        session = SessionFactory.for_schema_base(StateBase)
        session.add(person1)
        session.add(person2)
        session.commit()

        ingested_person1 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID,
                    id_type=external_id_types.US_ND_SID,
                    state_code=_STATE_CODE,
                )])

        ingested_person2 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID2,
                    id_type=external_id_types.US_ND_SID,
                    state_code=_STATE_CODE,
                )])

        ingested_person3 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    external_id='NONEXISTENT_ID',
                    id_type=external_id_types.US_ND_SID,
                    state_code=_STATE_CODE,
                )])

        # Act
        people = dao.read_people_by_external_ids(
            session, _REGION, [ingested_person1,
                               ingested_person2,
                               ingested_person3])

        # Assert
        expected_people = [person1, person2]

        self.assertCountEqual(people, expected_people)

    def test_readMultipleIngestedPeopleMatchSamePerson(self) -> None:
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

        session = SessionFactory.for_schema_base(StateBase)
        session.add(person)
        session.commit()

        ingested_person1 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID,
                    id_type=external_id_types.US_ND_SID,
                    state_code=_STATE_CODE,
                )])

        ingested_person2 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    external_id=_EXTERNAL_ID2,
                    id_type=external_id_types.US_ND_SID,
                    state_code=_STATE_CODE,
                )])

        # Act
        people = dao.read_people_by_external_ids(
            session, _REGION, [ingested_person1,
                               ingested_person2])

        # Assert
        expected_people = [person]

        self.assertCountEqual(people, expected_people)

    def test_readObjsWithExternalIdMatch(self) -> None:
        person_1 = schema.StatePerson(person_id=1, state_code=_STATE_CODE)
        sentence_group_1 = schema.StateSentenceGroup(
            sentence_group_id=1,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            person=person_1)
        person_1.sentence_groups = [sentence_group_1]

        person_2 = schema.StatePerson(person_id=2, state_code=_STATE_CODE)
        sentence_group_1_dup = schema.StateSentenceGroup(
            sentence_group_id=2,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            person=person_2)
        sentence_group_2 = schema.StateSentenceGroup(
            sentence_group_id=3,
            external_id=_EXTERNAL_ID2,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            person=person_2)
        placeholder_sentence_group = schema.StateSentenceGroup(
            sentence_group_id=4,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            person=person_2)

        person_2.sentence_groups = [sentence_group_1_dup,
                                    sentence_group_2,
                                    placeholder_sentence_group]

        session = SessionFactory.for_schema_base(StateBase)
        session.add(person_1)
        session.add(person_2)
        session.commit()

        # Act
        external_ids = dao.read_external_ids_of_cls_with_external_id_match(
            session, _STATE_CODE, schema.StateSentenceGroup)

        # Assert
        self.assertEqual(external_ids, [_EXTERNAL_ID])
