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
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.database import database_utils
from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.database.schema.state import schema
from recidiviz.tests.utils import fakes

_REGION = 'region'
_FULL_NAME = 'full_name'
_EXTERNAL_ID = 'external_id'
_BIRTHDATE = datetime.date(year=2012, month=1, day=2)


class TestDao(TestCase):
    """Test that the methods in dao.py correctly read from the SQL database."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def test_readPeople_byFullName(self):
        # Arrange
        person = schema.Person(person_id=8, full_name=_FULL_NAME)
        person_different_name = schema.Person(person_id=9,
                                              full_name='diff_name')

        session = Session()
        session.add(person)
        session.add(person_different_name)
        session.commit()

        # Act
        people = dao.read_people(session, full_name=_FULL_NAME, birthdate=None)

        # Assert
        self.assertEqual(people, [database_utils.convert(person)])

    def test_readPeople_byBirthdate(self):
        # Arrange
        person = schema.Person(person_id=8, birthdate=_BIRTHDATE)
        person_different_birthdate = schema.Person(person_id=9,
                                                   birthdate=datetime.date(
                                                       year=2002,
                                                       month=1,
                                                       day=2))

        session = Session()
        session.add(person)
        session.add(person_different_birthdate)
        session.commit()

        # Act
        people = dao.read_people(session, full_name=None, birthdate=_BIRTHDATE)

        # Assert
        self.assertEqual(people, [database_utils.convert(person)])

    def test_readPeople(self):
        # Arrange
        person = schema.Person(person_id=8,
                               full_name=_FULL_NAME,
                               birthdate=_BIRTHDATE)
        person_different_name = schema.Person(person_id=9,
                                              full_name='diff_name')
        person_different_birthdate = schema.Person(person_id=10,
                                                   birthdate=datetime.date(
                                                       year=2002,
                                                       month=1,
                                                       day=2)
                                                   )
        session = Session()
        session.add(person)
        session.add(person_different_name)
        session.add(person_different_birthdate)
        session.commit()

        # Act
        people = dao.read_people(session, full_name=None, birthdate=None)

        # Assert
        self.assertEqual(people,
                         [database_utils.convert(person),
                          database_utils.convert(person_different_name),
                          database_utils.convert(person_different_birthdate)])

    def test_readPeopleByExternalId(self):
        person_no_match = schema.Person(person_id=1)
        person_match_external_id = schema.Person(person_id=2)
        person_external_id = schema.PersonExternalId(
            person_external_id_id=1,
            external_id=_EXTERNAL_ID,
            state_code='us_ca',
            person_id=2,
        )

        session = Session()
        session.add(person_no_match)
        session.add(person_match_external_id)
        session.add(person_external_id)
        session.commit()

        ingested_person = entities.Person.new_with_defaults(
            external_ids=[entities.PersonExternalId.new_with_defaults(
                external_id=_EXTERNAL_ID,
                state_code='us_ca',
            )]
        )
        people = dao.read_people_by_external_ids(session, _REGION,
                                                 [ingested_person])

        expected_people = [
            database_utils.convert(person_match_external_id)]
        self.assertCountEqual(people, expected_people)

        # TODO(1625): Verify values in |people| are expected
        # TODO(1625): Write more tests where multiple ids match one person or we
        #  querying for multiple different people.
