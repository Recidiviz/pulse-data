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
"""Tests for converting state aliases."""
import unittest

from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import state_alias
from recidiviz.tests.persistence.database.database_test_utils import FakeIngestMetadata

_EMPTY_METADATA = FakeIngestMetadata.for_state("us_nd")


class StateAliasConverterTest(unittest.TestCase):
    """Tests for converting state aliases."""

    def testParseStateAliasNameParts(self):
        # Arrange
        ingest_alias = ingest_info_pb2.StateAlias(
            state_alias_id="ALIAS_ID",
            state_code="US_ND",
            given_names="FRANK",
            middle_names="LONNY",
            surname="OCEAN",
            name_suffix="BREAUX",
            alias_type="GIVEN",
        )

        # Act
        result = state_alias.convert(ingest_alias, _EMPTY_METADATA)

        # Assert
        expected_result = entities.StatePersonAlias(
            state_code="US_ND",
            full_name='{"given_names": "FRANK", '
            '"middle_names": "LONNY", '
            '"name_suffix": "BREAUX", '
            '"surname": "OCEAN"}',
            alias_type=StatePersonAliasType.GIVEN_NAME,
            alias_type_raw_text="GIVEN",
        )

        self.assertEqual(result, expected_result)

    def testParseStateAliasFullName(self):
        # Arrange
        ingest_alias = ingest_info_pb2.StateAlias(
            state_code="US_ND",
            state_alias_id="ALIAS_ID",
            full_name="FRANK OCEAN",
            alias_type="NICKNAME",
        )

        # Act
        result = state_alias.convert(ingest_alias, _EMPTY_METADATA)

        # Assert
        expected_result = entities.StatePersonAlias(
            state_code="US_ND",
            full_name='{"full_name": "FRANK OCEAN"}',
            alias_type=StatePersonAliasType.NICKNAME,
            alias_type_raw_text="NICKNAME",
        )

        self.assertEqual(result, expected_result)

    def testParseStateAliasTooManyNames(self):
        # Arrange
        ingest_alias = ingest_info_pb2.StateAlias(
            state_alias_id="ALIAS_ID",
            full_name="FRANK OCEAN",
            given_names="FRANK",
            middle_names="LONNY",
            surname="OCEAN",
            name_suffix="BREAUX",
            alias_type="NICKNAME",
        )

        # Act
        with self.assertRaises(ValueError):
            state_alias.convert(ingest_alias, _EMPTY_METADATA)
