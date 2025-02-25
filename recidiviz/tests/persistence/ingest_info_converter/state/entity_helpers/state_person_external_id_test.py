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
"""Tests for converting state person external ids."""

import unittest

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import \
    state_person_external_id

_EMPTY_METADATA = IngestMetadata.new_with_defaults()


class StatePersonExternalIdConverterTest(unittest.TestCase):
    """Tests for converting state person external ids."""

    def testParseStatePersonExternalId(self):
        # Arrange
        ingest_external_id = ingest_info_pb2.StatePersonExternalId(
            external_id='12345',
            id_type='state_id',
            state_code='us_nd',
        )

        # Act
        result = state_person_external_id.convert(ingest_external_id,
                                                  _EMPTY_METADATA)

        # Assert
        expected_result = entities.StatePersonExternalId(
            external_id='12345',
            id_type='STATE_ID',
            state_code='US_ND',
        )

        self.assertEqual(result, expected_result)
