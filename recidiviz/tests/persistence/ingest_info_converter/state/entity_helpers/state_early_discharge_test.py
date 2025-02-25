# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for converting state early discharges."""

import unittest
from datetime import date

from recidiviz.common.constants.state.state_early_discharge import StateEarlyDischargeDecision
from recidiviz.common.constants.state.shared_enums import StateActingBodyType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import state_early_discharge

_EMPTY_METADATA = IngestMetadata.new_with_defaults()


class StateEarlyDischargeConverterTest(unittest.TestCase):
    """Tests for converting state early discharges."""

    def testParseStateSupervisionViolationResponse(self):
        # Arrange
        ingest_response = ingest_info_pb2.StateEarlyDischarge(
            state_early_discharge_id='id1',
            request_date='2010/07/01',
            decision_date='2010/08/01',
            decision='REQUEST_DENIED',
            deciding_body_type='COURT',
            requesting_body_type='SUPERVISION_OFFICER',
            state_code='us_nd',
            county_code='cty',
        )

        # Act
        response_builder = entities.StateEarlyDischarge.builder()
        state_early_discharge.copy_fields_to_builder(response_builder, ingest_response, _EMPTY_METADATA)
        result = response_builder.build()

        # Assert
        expected_result = entities.StateEarlyDischarge.new_with_defaults(
            external_id='ID1',
            request_date=date(year=2010, month=7, day=1),
            decision_date=date(year=2010, month=8, day=1),
            decision=StateEarlyDischargeDecision.REQUEST_DENIED,
            decision_raw_text='REQUEST_DENIED',
            deciding_body_type=StateActingBodyType.COURT,
            deciding_body_type_raw_text='COURT',
            requesting_body_type=StateActingBodyType.SUPERVISION_OFFICER,
            requesting_body_type_raw_text='SUPERVISION_OFFICER',
            state_code='US_ND',
            county_code='CTY',
        )

        self.assertEqual(result, expected_result)
