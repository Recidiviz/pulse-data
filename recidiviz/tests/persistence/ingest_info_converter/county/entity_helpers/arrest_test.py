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
"""Tests for converting arrests."""
import unittest
from datetime import date

from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.county import entities
from recidiviz.persistence.ingest_info_converter.county.entity_helpers import arrest


class ArrestConverterTest(unittest.TestCase):
    """Tests for converting arrests."""

    def testParseArrest(self):
        # Arrange
        ingest_arrest = ingest_info_pb2.Arrest(
            arrest_id="ARREST_ID",
            arrest_date="1/2/1111",
            location="FAKE_LOCATION",
            officer_name="FAKE_NAME",
            officer_id="FAKE_ID",
            agency="FAKE_AGENCY",
        )

        # Act
        result = arrest.convert(ingest_arrest)

        # Assert
        expected_result = entities.Arrest(
            external_id="ARREST_ID",
            arrest_date=date(year=1111, month=1, day=2),
            location="FAKE_LOCATION",
            officer_name="FAKE_NAME",
            officer_id="FAKE_ID",
            agency="FAKE_AGENCY",
        )

        self.assertEqual(result, expected_result)

    def testParseArrest_LongAgencyName_TruncatesName(self):
        # Arrange
        ingest_arrest = ingest_info_pb2.Arrest(
            agency="Loooooooooooooooooooooooooooooooooooooooooooooooooo"
            "ooooooooooooooooooooooooooooooooooooooooooooooooooo"
            "ooooooooooooooooooooooooooooooooooooooooooooooooooo"
            "ooooooooooooooooooooooooooooooooooooooooooooooooooo"
            "ooooooooooooooooooooooooooooooooooooooooooooooooooo"
            "ooooooooooooooooooooooooooooooooooooooooooooooooooo"
            "ooooooooooooooooooooooooooooooooooooooooooooooooooo"
            "ooooooooooooooooooooooooooooooooooooooooooooooooooo"
            "ooooooooooooooooooooooooooooooooooooooooooooooooooo"
            "ooooooong Agency Name"
        )

        # Act
        result = arrest.convert(ingest_arrest)

        # Assert
        expected_result = entities.Arrest(
            agency="LOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO"
            "OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO"
            "OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO"
            "OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO"
            "OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO",
            external_id=None,
            arrest_date=None,
            location=None,
            officer_name=None,
            officer_id=None,
        )
        self.assertEqual(result, expected_result)
