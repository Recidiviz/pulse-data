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

"""Tests for converting state people."""

import unittest
from datetime import date, datetime

from mock import patch

from recidiviz.common.constants.person_characteristics import Gender, \
    ResidencyStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import \
    state_person

_NOW = datetime(2000, 5, 15)


class StatePersonConverterTest(unittest.TestCase):
    """Tests for converting state people."""

    def setUp(self):
        self.subject = entities.StatePerson.builder()

    def testParsesStatePerson(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults(
            region='us_nd', jurisdiction_id='JURISDICTION_ID')
        ingest_person = ingest_info_pb2.StatePerson(
            gender='MALE',
            full_name='FULL_NAME',
            birthdate='12-31-1999',
            current_address='NNN\n  STREET \t ZIP'
        )

        # Act
        state_person.copy_fields_to_builder(
            self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.StatePerson.new_with_defaults(
            gender=Gender.MALE,
            gender_raw_text='MALE',
            full_name='{"full_name": "FULL_NAME"}',
            birthdate=date(year=1999, month=12, day=31),
            birthdate_inferred_from_age=False,
            current_address='NNN STREET ZIP',
            residency_status=ResidencyStatus.PERMANENT,
        )

        self.assertEqual(result, expected_result)

    def testParseStatePerson_WithSurnameAndFullname_ThrowsException(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults()
        ingest_person = ingest_info_pb2.StatePerson(
            full_name='LAST,FIRST',
            surname='LAST'
        )

        # Arrange + Act
        with self.assertRaises(ValueError):
            state_person.copy_fields_to_builder(
                self.subject, ingest_person, metadata)

    def testParseStatePerson_WithSurnameAndGivenNames_UsesFullNameAsJson(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults()
        ingest_person = ingest_info_pb2.StatePerson(
            surname='UNESCAPED,SURNAME"WITH-CHARS"',
            given_names='GIVEN_NAMES',
            middle_names='MIDDLE_NAMES'
        )

        # Act
        state_person.copy_fields_to_builder(
            self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_full_name = \
            '{{"given_names": "{}", "middle_names": "{}", "surname": "{}"}}'\
            .format('GIVEN_NAMES', 'MIDDLE_NAMES',
                    'UNESCAPED,SURNAME\\"WITH-CHARS\\"')
        expected_result = entities.StatePerson.new_with_defaults(
            full_name=expected_full_name)

        self.assertEqual(result, expected_result)

    @patch('recidiviz.persistence.ingest_info_converter.utils.converter_utils.'
           'datetime.datetime')
    def testParseStatePerson_InfersBirthdateFromAge(self, mock_datetime):
        # Arrange
        mock_datetime.now.return_value = _NOW
        metadata = IngestMetadata.new_with_defaults()
        ingest_person = ingest_info_pb2.StatePerson(age='27')

        # Act
        state_person.copy_fields_to_builder(
            self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.StatePerson.new_with_defaults(
            birthdate=datetime(year=_NOW.year - 27, month=1, day=1).date(),
            birthdate_inferred_from_age=True
        )

        self.assertEqual(result, expected_result)

    def testParseStatePerson_TakesLastZipCodeMatch(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults(region='us_nd')
        # 5-digit address could be mistaken for a zip code
        ingest_person = ingest_info_pb2.StatePerson(
            current_address='12345 Main 58503')

        # Act
        state_person.copy_fields_to_builder(
            self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.StatePerson.new_with_defaults(
            current_address='12345 MAIN 58503',
            residency_status=ResidencyStatus.PERMANENT
        )

        self.assertEqual(result, expected_result)

    def testParseStatePerson_NoiseInPlaceOfResidence_ParsesResidency(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults(region='us_ky_allen')
        ingest_person = ingest_info_pb2.StatePerson(
            current_address='transient moves around')

        # Act
        state_person.copy_fields_to_builder(
            self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.StatePerson.new_with_defaults(
            current_address='TRANSIENT MOVES AROUND',
            residency_status=ResidencyStatus.TRANSIENT
        )

        self.assertEqual(result, expected_result)
