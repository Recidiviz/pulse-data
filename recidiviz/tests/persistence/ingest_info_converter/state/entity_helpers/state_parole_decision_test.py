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
"""Tests for converting state parole decisions."""

import unittest
from datetime import date

from recidiviz.common.constants.state.state_parole_decision import (
    StateParoleDecisionOutcome,
)
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_parole_decision,
)
from recidiviz.tests.persistence.database.database_test_utils import FakeIngestMetadata

_EMPTY_METADATA = FakeIngestMetadata.for_state("us_nd")


class StateParoleDecisionConverterTest(unittest.TestCase):
    """Tests for converting parole decisions."""

    def testParseParoleDecision(self):
        # Arrange
        ingest_parole_decision = ingest_info_pb2.StateParoleDecision(
            state_parole_decision_id="PAROLE_DECISION_ID",
            decision_date="1/2/2111",
            corrective_action_deadline="2/2/2111",
            state_code="us_nd",
            county_code=None,
            decision_outcome="PAROLE GRANTED",
            decision_reasoning="GREAT ALL AROUND PERSON",
            corrective_action=None,
        )

        # Act
        decision_builder = entities.StateParoleDecision.builder()
        state_parole_decision.copy_fields_to_builder(
            decision_builder, ingest_parole_decision, _EMPTY_METADATA
        )
        result = decision_builder.build()

        # Assert
        expected_result = entities.StateParoleDecision(
            external_id="PAROLE_DECISION_ID",
            decision_date=date(year=2111, month=1, day=2),
            corrective_action_deadline=date(year=2111, month=2, day=2),
            state_code="US_ND",
            county_code=None,
            decision_outcome=StateParoleDecisionOutcome.PAROLE_GRANTED,
            decision_outcome_raw_text="PAROLE GRANTED",
            decision_reasoning="GREAT ALL AROUND PERSON",
            corrective_action=None,
        )

        self.assertEqual(result, expected_result)
