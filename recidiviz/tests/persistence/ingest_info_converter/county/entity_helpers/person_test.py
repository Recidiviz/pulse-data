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
"""Tests for converting people."""
import unittest
from datetime import date, datetime

from mock import Mock, patch

from recidiviz.common.constants.person_characteristics import (
    Ethnicity,
    Gender,
    Race,
    ResidencyStatus,
)
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.persistence.entity.county import entities
from recidiviz.persistence.ingest_info_converter.county.entity_helpers import person
from recidiviz.tests.persistence.database.database_test_utils import (
    FakeLegacyStateAndJailsIngestMetadata,
)

_NOW = datetime(2000, 5, 15)


class PersonConverterTest(unittest.TestCase):
    """Tests for converting people."""

    def setUp(self):
        self.subject = entities.Person.builder()

    def testParsesPerson(self):
        # Arrange
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region="REGION", jurisdiction_id="JURISDICTION_ID"
        )
        ingest_person = ingest_info_pb2.Person(
            full_name="FULL_NAME",
            birthdate="12-31-1999",
            gender="MALE",
            race="WHITE",
            ethnicity="HISPANIC",
            place_of_residence="NNN\n  STREET \t ZIP",
        )

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Person.new_with_defaults(
            full_name='{"full_name": "FULL_NAME"}',
            birthdate=date(year=1999, month=12, day=31),
            birthdate_inferred_from_age=False,
            gender=Gender.MALE,
            gender_raw_text="MALE",
            race=Race.WHITE,
            race_raw_text="WHITE",
            ethnicity=Ethnicity.HISPANIC,
            ethnicity_raw_text="HISPANIC",
            residency_status=ResidencyStatus.PERMANENT,
            region="REGION",
            jurisdiction_id="JURISDICTION_ID",
        )

        self.assertEqual(result, expected_result)

    def testParsePerson_WithSurnameAndFullname_ThrowsException(self):
        # Arrange
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(region="REGION")
        ingest_person = ingest_info_pb2.Person(full_name="LAST,FIRST", surname="LAST")

        # Arrange + Act
        with self.assertRaises(ValueError):
            person.copy_fields_to_builder(self.subject, ingest_person, metadata)

    def testParsePerson_WithSurnameAndGivenNames_UsesFullNameAsJson(self):
        # Arrange
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region="REGION", jurisdiction_id="JURISDICTION_ID"
        )
        ingest_person = ingest_info_pb2.Person(
            surname='UNESCAPED,SURNAME"WITH-CHARS"',
            given_names="GIVEN_NAMES",
            middle_names="MIDDLE_NAMES",
        )

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_full_name = '{"given_names": "GIVEN_NAMES", "middle_names": "MIDDLE_NAMES", "surname": "UNESCAPED,SURNAME\\"WITH-CHARS\\""}'
        expected_result = entities.Person.new_with_defaults(
            region="REGION",
            jurisdiction_id="JURISDICTION_ID",
            full_name=expected_full_name,
        )

        self.assertEqual(result, expected_result)

    def testParsePerson_NoNames_FullNameIsNone(self):
        # Arrange
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region="REGION", jurisdiction_id="JURISDICTION_ID"
        )
        ingest_person = ingest_info_pb2.Person(person_id="1234")

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Person.new_with_defaults(
            region="REGION", jurisdiction_id="JURISDICTION_ID", external_id="1234"
        )

        self.assertEqual(result, expected_result)

    @patch(
        "recidiviz.persistence.ingest_info_converter.utils.converter_utils."
        "datetime.datetime"
    )
    def testParsePerson_InfersBirthdateFromAge(self, mock_datetime):
        # Arrange
        mock_datetime.now.return_value = _NOW
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region="REGION", jurisdiction_id="JURISDICTION_ID"
        )
        ingest_person = ingest_info_pb2.Person(age="27")

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Person.new_with_defaults(
            region="REGION",
            jurisdiction_id="JURISDICTION_ID",
            birthdate=datetime(year=_NOW.year - 27, month=1, day=1).date(),
            birthdate_inferred_from_age=True,
        )

        self.assertEqual(result, expected_result)

    def testParsePerson_RaceIsEthnicity(self):
        class ScraperWithDefaultOverrides(BaseScraper):
            """Class created so BaseScraper's enum_overrides can be used."""

            def __init__(self):  # pylint: disable=super-init-not-called
                self.region = Mock()

            def populate_data(self, _, __, ___):
                pass

        # Arrange
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region="REGION",
            jurisdiction_id="JURISDICTION_ID",
            enum_overrides=ScraperWithDefaultOverrides().get_enum_overrides(),
        )
        ingest_person = ingest_info_pb2.Person(race="HISPANIC")

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Person.new_with_defaults(
            region="REGION",
            jurisdiction_id="JURISDICTION_ID",
            ethnicity=Ethnicity.HISPANIC,
            race_raw_text="HISPANIC",
        )

        self.assertEqual(result, expected_result)

    def testParsePerson_ResidentOfCounty(self):
        # Arrange
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region="us_ky_allen", jurisdiction_id="JURISDICTION_ID"
        )
        # 42164 is in Allen
        ingest_person = ingest_info_pb2.Person(place_of_residence="123 Main 42164")

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Person.new_with_defaults(
            resident_of_region=True,
            residency_status=ResidencyStatus.PERMANENT,
            region="us_ky_allen",
            jurisdiction_id="JURISDICTION_ID",
        )

        self.assertEqual(result, expected_result)

    def testParsePerson_NotResidentOfCounty(self):
        # Arrange
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region="us_ky_allen", jurisdiction_id="JURISDICTION_ID"
        )
        # 40601 is in Frankfort
        ingest_person = ingest_info_pb2.Person(place_of_residence="123 Main 40601")

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Person.new_with_defaults(
            resident_of_region=False,
            residency_status=ResidencyStatus.PERMANENT,
            region="us_ky_allen",
            jurisdiction_id="JURISDICTION_ID",
        )

        self.assertEqual(result, expected_result)

    def testParsePerson_ResidentOfState(self):
        # Arrange
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region="us_ky", jurisdiction_id="JURISDICTION_ID"
        )
        # 42164 is in Allen
        ingest_person = ingest_info_pb2.Person(place_of_residence="123 Main 42164")

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Person.new_with_defaults(
            resident_of_region=True,
            residency_status=ResidencyStatus.PERMANENT,
            region="us_ky",
            jurisdiction_id="JURISDICTION_ID",
        )

        self.assertEqual(result, expected_result)

    def testParsePerson_NotResidentOfState(self):
        # Arrange
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region="us_ky", jurisdiction_id="JURISDICTION_ID"
        )
        # 10011 is in New York
        ingest_person = ingest_info_pb2.Person(place_of_residence="123 Main 10011")

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Person.new_with_defaults(
            resident_of_region=False,
            residency_status=ResidencyStatus.PERMANENT,
            region="us_ky",
            jurisdiction_id="JURISDICTION_ID",
        )

        self.assertEqual(result, expected_result)

    def testParsePerson_TakesLastZipCodeMatch(self):
        # Arrange
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region="us_ky_allen", jurisdiction_id="JURISDICTION_ID"
        )
        # 5-digit address could be mistaken for a zip code
        ingest_person = ingest_info_pb2.Person(place_of_residence="12345 Main 42164")

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Person.new_with_defaults(
            resident_of_region=True,
            residency_status=ResidencyStatus.PERMANENT,
            region="us_ky_allen",
            jurisdiction_id="JURISDICTION_ID",
        )

        self.assertEqual(result, expected_result)

    def testParsePerson_NoiseInPlaceOfResidence_ParsesResidencyStatus(self):
        # Arrange
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region="us_ky_allen", jurisdiction_id="JURISDICTION_ID"
        )
        ingest_person = ingest_info_pb2.Person(
            place_of_residence="transient moves around"
        )

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Person.new_with_defaults(
            residency_status=ResidencyStatus.TRANSIENT,
            region="us_ky_allen",
            jurisdiction_id="JURISDICTION_ID",
        )

        self.assertEqual(result, expected_result)

    def testParsePerson_ResidenceAndStatusCombined(self):
        # Arrange
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region="us_ky_allen", jurisdiction_id="JURISDICTION_ID"
        )
        ingest_person = ingest_info_pb2.Person(place_of_residence="42164 homeless")

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Person.new_with_defaults(
            resident_of_region=True,
            residency_status=ResidencyStatus.HOMELESS,
            region="us_ky_allen",
            jurisdiction_id="JURISDICTION_ID",
        )

        self.assertEqual(result, expected_result)
