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

import pytest

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import \
    state_alias


_EMPTY_METADATA = IngestMetadata.new_with_defaults()


class StateAliasConverterTest(unittest.TestCase):
    """Tests for converting state aliases."""

    def testParseStateAliasNameParts(self):
        # Arrange
        ingest_alias = ingest_info_pb2.StateAlias(
            alias_id='ALIAS_ID',
            state_code='US_ND',
            given_names='FRANK',
            middle_names='LONNY',
            surname='OCEAN',
            name_suffix='BREAUX',
        )

        # Act
        result = state_alias.convert(ingest_alias, _EMPTY_METADATA)

        # Assert
        expected_result = entities.StatePersonAlias(
            state_code='US_ND',
            full_name='{"given_names": "FRANK", '
                      '"middle_names": "LONNY", '
                      '"name_suffix": "BREAUX", '
                      '"surname": "OCEAN"}',
            given_names='FRANK',
            middle_names='LONNY',
            surname='OCEAN',
            name_suffix='BREAUX',
        )

        self.assertEqual(result, expected_result)

    def testParseStateAliasFullName(self):
        # Arrange
        ingest_alias = ingest_info_pb2.StateAlias(
            state_code='US_ND',
            alias_id='ALIAS_ID',
            full_name='FRANK OCEAN',
        )

        # Act
        result = state_alias.convert(ingest_alias, _EMPTY_METADATA)

        # Assert
        expected_result = entities.StatePersonAlias(
            state_code='US_ND',
            full_name='{"full_name": "FRANK OCEAN"}',
        )

        self.assertEqual(result, expected_result)

    @staticmethod
    def testParseStateAliasTooManyNames():
        # Arrange
        ingest_alias = ingest_info_pb2.StateAlias(
            alias_id='ALIAS_ID',
            full_name='FRANK OCEAN',
            given_names='FRANK',
            middle_names='LONNY',
            surname='OCEAN',
            name_suffix='BREAUX',
        )

        # Act
        with pytest.raises(ValueError):
            state_alias.convert(ingest_alias, _EMPTY_METADATA)
