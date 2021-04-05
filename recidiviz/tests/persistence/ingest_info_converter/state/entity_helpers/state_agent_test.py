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
"""Tests for converting state agents."""

import unittest

from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import state_agent
from recidiviz.tests.persistence.database.database_test_utils import FakeIngestMetadata

_EMPTY_METADATA = FakeIngestMetadata.for_state("us_nd")


class StateAgentConverterTest(unittest.TestCase):
    """Tests for converting state agents."""

    def testParseStateAgent(self):
        # Arrange
        ingest_agent = ingest_info_pb2.StateAgent(
            agent_type="JUDGE",
            state_agent_id="AGENT_ID",
            state_code="us_nd",
            full_name="Judge Joe Brown",
        )

        # Act
        result = state_agent.convert(ingest_agent, _EMPTY_METADATA)

        # Assert
        expected_result = entities.StateAgent(
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text="JUDGE",
            external_id="AGENT_ID",
            state_code="US_ND",
            full_name='{"full_name": "JUDGE JOE BROWN"}',
        )

        self.assertEqual(result, expected_result)

    def testParseStateAgentNoType(self):
        # Arrange
        ingest_agent = ingest_info_pb2.StateAgent(
            state_agent_id="AGENT_ID", state_code="us_nd"
        )

        # Act
        result = state_agent.convert(ingest_agent, _EMPTY_METADATA)

        # Assert
        expected_result = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
            external_id="AGENT_ID",
            state_code="US_ND",
        )

        self.assertEqual(result, expected_result)
