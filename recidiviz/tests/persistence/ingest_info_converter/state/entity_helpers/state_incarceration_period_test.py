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
"""Tests for converting state incarceration periods."""

import unittest
from datetime import date

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.deserialize_entity_factories import (
    StateIncarcerationPeriodFactory,
)
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_incarceration_period,
)
from recidiviz.tests.persistence.database.database_test_utils import (
    FakeLegacyStateIngestMetadata,
)

_EMPTY_METADATA = FakeLegacyStateIngestMetadata.for_state("us_nd")


class StateIncarcerationPeriodConverterTest(unittest.TestCase):
    """Tests for converting incarcerations."""

    def testParseStateIncarcerationPeriod(self):
        # Arrange
        ingest_incarceration = ingest_info_pb2.StateIncarcerationPeriod(
            incarceration_type="STATE_PRISON",
            admission_reason="REVOCATION",
            release_reason="SERVED",
            state_incarceration_period_id="INCARCERATION_ID",
            specialized_purpose_for_incarceration="SHOCK INCARCERATION",
            admission_date="1/2/2111",
            release_date="2/2/2112",
            state_code="us_nd",
            county_code="bis",
            facility="The Prison",
            housing_unit="CB4",
            custodial_authority="STATE_PRISON",
        )

        # Act
        incarceration_builder = entities.StateIncarcerationPeriod.builder()
        state_incarceration_period.copy_fields_to_builder(
            incarceration_builder, ingest_incarceration, _EMPTY_METADATA
        )
        result = incarceration_builder.build(
            StateIncarcerationPeriodFactory.deserialize
        )

        # Assert
        expected_result = entities.StateIncarcerationPeriod(
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="STATE_PRISON",
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="REVOCATION",
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            release_reason_raw_text="SERVED",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            specialized_purpose_for_incarceration_raw_text="SHOCK INCARCERATION",
            external_id="INCARCERATION_ID",
            admission_date=date(year=2111, month=1, day=2),
            release_date=date(year=2112, month=2, day=2),
            state_code="US_ND",
            county_code="BIS",
            facility="THE PRISON",
            housing_unit="CB4",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="STATE_PRISON",
        )

        self.assertEqual(result, expected_result)

    def testParseStateIncarcerationPeriod_inferStatus_noDates(self):
        # Arrange
        ingest_incarceration = ingest_info_pb2.StateIncarcerationPeriod(
            incarceration_type="STATE_PRISON", state_code="us_nd"
        )

        # Act
        incarceration_builder = entities.StateIncarcerationPeriod.builder()
        state_incarceration_period.copy_fields_to_builder(
            incarceration_builder, ingest_incarceration, _EMPTY_METADATA
        )
        result = incarceration_builder.build(
            StateIncarcerationPeriodFactory.deserialize
        )

        # Assert
        expected_result = entities.StateIncarcerationPeriod.new_with_defaults(
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="STATE_PRISON",
            state_code="US_ND",
        )

        self.assertEqual(result, expected_result)

    def testParseStateIncarcerationPeriod_inferStatus_releaseDate(self):
        # Arrange
        ingest_incarceration = ingest_info_pb2.StateIncarcerationPeriod(
            incarceration_type="STATE_PRISON",
            admission_date="1/2/2111",
            release_date="1/2/2112",
            state_code="us_nd",
        )

        # Act
        incarceration_builder = entities.StateIncarcerationPeriod.builder()
        state_incarceration_period.copy_fields_to_builder(
            incarceration_builder, ingest_incarceration, _EMPTY_METADATA
        )
        result = incarceration_builder.build(
            StateIncarcerationPeriodFactory.deserialize
        )

        # Assert
        expected_result = entities.StateIncarcerationPeriod.new_with_defaults(
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="STATE_PRISON",
            admission_date=date(year=2111, month=1, day=2),
            release_date=date(year=2112, month=1, day=2),
            state_code="US_ND",
        )

        self.assertEqual(result, expected_result)

    def testParseStateIncarcerationPeriod_inferStatus_admissionNoRelease(self):
        # Arrange
        ingest_incarceration = ingest_info_pb2.StateIncarcerationPeriod(
            incarceration_type="STATE_PRISON",
            admission_date="1/2/2111",
            state_code="us_nd",
        )

        # Act
        incarceration_builder = entities.StateIncarcerationPeriod.builder()
        state_incarceration_period.copy_fields_to_builder(
            incarceration_builder, ingest_incarceration, _EMPTY_METADATA
        )
        result = incarceration_builder.build(
            StateIncarcerationPeriodFactory.deserialize
        )

        # Assert
        expected_result = entities.StateIncarcerationPeriod.new_with_defaults(
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="STATE_PRISON",
            admission_date=date(year=2111, month=1, day=2),
            state_code="US_ND",
        )

        self.assertEqual(result, expected_result)
