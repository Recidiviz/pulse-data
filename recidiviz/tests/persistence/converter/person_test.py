# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
"""Tests for converting people."""
import unittest
from datetime import datetime

from mock import patch

from recidiviz.common.constants.person import Gender, Race, Ethnicity
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence import entities
from recidiviz.persistence.converter import person

_NOW = datetime(2000, 5, 15)


class PersonConverterTest(unittest.TestCase):
    """Tests for converting people."""

    def testParsesPerson(self):
        # Arrange
        ingest_person = ingest_info_pb2.Person(
            full_name='LAST,FIRST',
            birthdate='12-31-1999',
            gender='MALE',
            race='WHITE',
            ethnicity='HISPANIC',
            place_of_residence='NNN\n  STREET \t ZIP')

        # Act
        result = person.convert(ingest_person)

        # Assert
        expected_result = entities.Person(
            given_names='FIRST',
            surname='LAST',
            birthdate=datetime(year=1999, month=12, day=31),
            birthdate_inferred_from_age=False,
            gender=Gender.MALE,
            race=Race.WHITE,
            ethnicity=Ethnicity.HISPANIC,
            place_of_residence='NNN STREET ZIP'
        )

        self.assertEqual(result, expected_result)

    def testParsePerson_WithSurnameAndFullname_ThrowsException(self):
        # Arrange
        ingest_person = ingest_info_pb2.Person(
            full_name='LAST,FIRST',
            surname='LAST'
        )

        # Arrange + Act
        with self.assertRaises(ValueError):
            person.convert(ingest_person)

    def testParsePerson_UsesSurnameAndGivenNames(self):
        # Arrange
        ingest_person = ingest_info_pb2.Person(
            surname='SURNAME',
            given_names='GIVEN_NAMES'
        )

        # Act
        result = person.convert(ingest_person)

        # Assert
        expected_result = entities.Person(
            surname='SURNAME',
            given_names='GIVEN_NAMES'
        )

        self.assertEqual(result, expected_result)

    @patch('recidiviz.persistence.converter.converter_utils.datetime.datetime')
    def testParsePerson_InfersBirthdateFromAge(self, mock_datetime):
        # Arrange
        mock_datetime.now.return_value = _NOW
        ingest_person = ingest_info_pb2.Person(age='27')

        # Act
        result = person.convert(ingest_person)

        # Assert
        expected_result = entities.Person(
            birthdate=datetime(year=_NOW.year - 27, month=1, day=1).date(),
            birthdate_inferred_from_age=True
        )

        self.assertEqual(result, expected_result)

    def testParsePerson_RaceIsEthnicity(self):
        # Arrange
        ingest_person = ingest_info_pb2.Person(race='HISPANIC')

        # Act
        result = person.convert(ingest_person)

        # Assert
        expected_result = entities.Person(ethnicity=Ethnicity.HISPANIC)
        self.assertEqual(result, expected_result)
