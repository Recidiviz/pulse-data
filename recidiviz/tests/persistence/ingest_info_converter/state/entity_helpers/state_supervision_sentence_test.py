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

"""Tests for converting state supervision sentences."""

import unittest
from datetime import date

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_supervision_sentence,
)
from recidiviz.tests.persistence.database.database_test_utils import FakeIngestMetadata

METADATA = FakeIngestMetadata.for_state(region="us_nd")


class StateSupervisionSentenceConverterTest(unittest.TestCase):
    """Tests for converting state supervision sentences."""

    def testParseStateSupervisionSentence(self):
        # Arrange
        ingest_supervision = ingest_info_pb2.StateSupervisionSentence(
            status="SUSPENDED",
            supervision_type="PROBATION",
            state_supervision_sentence_id="SENTENCE_ID",
            date_imposed="2000-12-13",
            start_date="20010101",
            completion_date="1/2/2111",
            county_code="CO",
            min_length="90D",
            max_length="180D",
        )

        # Act
        supervision_builder = entities.StateSupervisionSentence.builder()
        state_supervision_sentence.copy_fields_to_builder(
            supervision_builder, ingest_supervision, METADATA
        )
        result = supervision_builder.build()

        # Assert
        expected_result = entities.StateSupervisionSentence(
            status=StateSentenceStatus.SUSPENDED,
            status_raw_text="SUSPENDED",
            supervision_type=StateSupervisionType.PROBATION,
            supervision_type_raw_text="PROBATION",
            external_id="SENTENCE_ID",
            date_imposed=date(year=2000, month=12, day=13),
            start_date=date(year=2001, month=1, day=1),
            completion_date=None,
            projected_completion_date=date(year=2111, month=1, day=2),
            state_code="US_ND",
            county_code="CO",
            min_length_days=90,
            max_length_days=180,
        )

        self.assertEqual(result, expected_result)
