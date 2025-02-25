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
"""Tests for converting state supervision violation responses."""

import unittest
from datetime import date

from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseType, \
    StateSupervisionViolationResponseDecision, \
    StateSupervisionViolationResponseRevocationType, \
    StateSupervisionViolationResponseDecidingBodyType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import \
    state_supervision_violation_response

_EMPTY_METADATA = IngestMetadata.new_with_defaults()


class StateSupervisionViolationResponseConverterTest(unittest.TestCase):
    """Tests for converting state supervision violations."""

    def testParseStateSupervisionViolationResponse(self):
        # Arrange
        ingest_response = ingest_info_pb2.StateSupervisionViolationResponse(
            response_type='PERMANENT_DECISION',
            decision='REVOCATION',
            revocation_type='REINCARCERATION',
            deciding_body_type='PAROLE_BOARD',
            state_supervision_violation_response_id='RESPONSE_ID',
            response_date='1/2/2111',
            state_code='us_nd'
        )

        # Act
        response_builder = entities.StateSupervisionViolationResponse.builder()
        state_supervision_violation_response.copy_fields_to_builder(
            response_builder, ingest_response, _EMPTY_METADATA)
        result = response_builder.build()

        # Assert
        expected_result = entities.StateSupervisionViolationResponse(
            response_type=
            StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text='PERMANENT_DECISION',
            decision=
            StateSupervisionViolationResponseDecision.REVOCATION,
            decision_raw_text='REVOCATION',
            revocation_type=
            StateSupervisionViolationResponseRevocationType.REINCARCERATION,
            revocation_type_raw_text='REINCARCERATION',
            deciding_body_type=
            StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text='PAROLE_BOARD',
            external_id='RESPONSE_ID',
            response_date=date(year=2111, month=1, day=2),
            state_code='US_ND'
        )

        self.assertEqual(result, expected_result)
