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

"""Tests for converting state sentence groups."""

import unittest
from datetime import date

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.deserialize_entity_factories import (
    StateSentenceGroupFactory,
)
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_sentence_group,
)
from recidiviz.tests.persistence.database.database_test_utils import (
    FakeLegacyStateAndJailsIngestMetadata,
)

METADATA = FakeLegacyStateAndJailsIngestMetadata.for_state("us_nd")


class StateSentenceGroupConverterTest(unittest.TestCase):
    """Tests for converting state sentence groups."""

    def testParseStateSentenceGroup(self):
        # Arrange
        ingest_group = ingest_info_pb2.StateSentenceGroup(
            status="SUSPENDED",
            state_sentence_group_id="GROUP_ID",
            date_imposed="1/2/2111",
            county_code="CO",
            min_length="200",
            max_length="600",
            is_life="false",
        )

        # Act
        group_builder = entities.StateSentenceGroup.builder()
        state_sentence_group.copy_fields_to_builder(
            group_builder, ingest_group, METADATA
        )
        result = group_builder.build(StateSentenceGroupFactory.deserialize)

        # Assert
        expected_result = entities.StateSentenceGroup(
            status=StateSentenceStatus.SUSPENDED,
            status_raw_text="SUSPENDED",
            external_id="GROUP_ID",
            date_imposed=date(year=2111, month=1, day=2),
            state_code="US_ND",
            county_code="CO",
            min_length_days=200,
            max_length_days=600,
            is_life=False,
        )

        self.assertEqual(result, expected_result)
