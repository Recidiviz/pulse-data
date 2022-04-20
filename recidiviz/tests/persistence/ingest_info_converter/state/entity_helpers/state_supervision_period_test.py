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

from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.deserialize_entity_factories import (
    StateSupervisionPeriodFactory,
)
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_supervision_period,
)
from recidiviz.tests.persistence.database.database_test_utils import (
    FakeLegacyStateAndJailsIngestMetadata,
)

_EMPTY_METADATA = FakeLegacyStateAndJailsIngestMetadata.for_state("US_XX")


class StateSupervisionPeriodConverterTest(unittest.TestCase):
    """Tests for converting supervision."""

    def testParseStateSupervisionPeriod(self):
        # Arrange
        ingest_supervision = ingest_info_pb2.StateSupervisionPeriod(
            supervision_type="PAROLE",
            admission_reason="CONDITIONAL_RELEASE",
            termination_reason="DISCHARGE",
            supervision_level=None,
            state_supervision_period_id="SUPERVISION_ID",
            start_date="1/2/2111",
            termination_date="2/2/2112",
            state_code="US_XX",
            county_code="bis",
            custodial_authority="SUPERVISION AUTHORITY",
            supervision_site="07-CENTRAL",
            conditions="CURFEW, DRINKING",
        )

        # Act
        supervision_builder = entities.StateSupervisionPeriod.builder()
        state_supervision_period.copy_fields_to_builder(
            supervision_builder, ingest_supervision, _EMPTY_METADATA
        )
        result = supervision_builder.build(StateSupervisionPeriodFactory.deserialize)

        # Assert
        expected_result = entities.StateSupervisionPeriod(
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type_raw_text="PAROLE",
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            admission_reason_raw_text="CONDITIONAL_RELEASE",
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            termination_reason_raw_text="DISCHARGE",
            supervision_level=None,
            supervision_level_raw_text=None,
            external_id="SUPERVISION_ID",
            start_date=date(year=2111, month=1, day=2),
            termination_date=date(year=2112, month=2, day=2),
            state_code="US_XX",
            county_code="BIS",
            supervision_site="07-CENTRAL",
            conditions="CURFEW, DRINKING",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="SUPERVISION AUTHORITY",
        )

        self.assertEqual(result, expected_result)

    def testParseStateSupervisionPeriod_inferStatus_noDates(self):
        # Arrange
        ingest_supervision = ingest_info_pb2.StateSupervisionPeriod(state_code="US_XX")

        # Act
        supervision_builder = entities.StateSupervisionPeriod.builder()
        state_supervision_period.copy_fields_to_builder(
            supervision_builder, ingest_supervision, _EMPTY_METADATA
        )
        result = supervision_builder.build(StateSupervisionPeriodFactory.deserialize)

        # Assert
        expected_result = entities.StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX"
        )

        self.assertEqual(expected_result, result)

    def testParseStateSupervisionPeriod_inferStatus_terminationDate(self):
        # Arrange
        ingest_supervision = ingest_info_pb2.StateSupervisionPeriod(
            state_code="US_XX",
            start_date="1/2/2111",
            termination_date="1/2/2112",
        )

        # Act
        supervision_builder = entities.StateSupervisionPeriod.builder()
        state_supervision_period.copy_fields_to_builder(
            supervision_builder, ingest_supervision, _EMPTY_METADATA
        )
        result = supervision_builder.build(StateSupervisionPeriodFactory.deserialize)

        # Assert
        expected_result = entities.StateSupervisionPeriod.new_with_defaults(
            start_date=date(year=2111, month=1, day=2),
            termination_date=date(year=2112, month=1, day=2),
            state_code="US_XX",
        )

        self.assertEqual(expected_result, result)

    def testParseStateSupervisionPeriod_inferStatus_startNoTermination(self):
        # Arrange
        ingest_supervision = ingest_info_pb2.StateSupervisionPeriod(
            state_code="US_XX",
            start_date="1/2/2111",
        )

        # Act
        supervision_builder = entities.StateSupervisionPeriod.builder()
        state_supervision_period.copy_fields_to_builder(
            supervision_builder, ingest_supervision, _EMPTY_METADATA
        )
        result = supervision_builder.build(StateSupervisionPeriodFactory.deserialize)

        # Assert
        expected_result = entities.StateSupervisionPeriod.new_with_defaults(
            start_date=date(year=2111, month=1, day=2),
            state_code="US_XX",
        )

        self.assertEqual(expected_result, result)
