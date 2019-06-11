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
"""Tests for converting state supervision periods."""

import unittest
from datetime import date

from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus, StateSupervisionPeriodAdmissionReason, \
    StateSupervisionPeriodTerminationReason
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import \
    state_supervision_period

_EMPTY_METADATA = IngestMetadata.new_with_defaults()


class StateSupervisionPeriodConverterTest(unittest.TestCase):
    """Tests for converting supervision."""

    def testParseStateSupervisionPeriod(self):
        # Arrange
        ingest_supervision = ingest_info_pb2.StateSupervisionPeriod(
            status='TERMINATED',
            supervision_type='PAROLE',
            admission_reason='CONDITIONAL_RELEASE',
            termination_reason='DISCHARGE',
            supervision_level=None,
            state_supervision_period_id='SUPERVISION_ID',
            start_date='1/2/2111',
            termination_date='2/2/2112',
            state_code='us_nd',
            county_code='bis',
            conditions=['CURFEW', 'DRINKING']
        )

        # Act
        supervision_builder = entities.StateSupervisionPeriod.builder()
        state_supervision_period.copy_fields_to_builder(
            supervision_builder, ingest_supervision, _EMPTY_METADATA)
        result = supervision_builder.build()

        # Assert
        expected_result = entities.StateSupervisionPeriod(
            status=StateSupervisionPeriodStatus.TERMINATED,
            status_raw_text='TERMINATED',
            supervision_type=StateSupervisionType.PAROLE,
            supervision_type_raw_text='PAROLE',
            admission_reason=
            StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            admission_reason_raw_text='CONDITIONAL_RELEASE',
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            termination_reason_raw_text='DISCHARGE',
            supervision_level=None,
            supervision_level_raw_text=None,
            external_id='SUPERVISION_ID',
            start_date=date(year=2111, month=1, day=2),
            termination_date=date(year=2112, month=2, day=2),
            state_code='US_ND',
            county_code='BIS',
            conditions='CURFEW, DRINKING',
        )

        self.assertEqual(result, expected_result)
