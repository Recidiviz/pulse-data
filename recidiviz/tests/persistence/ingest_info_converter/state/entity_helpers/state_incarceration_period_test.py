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

from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationFacilitySecurityLevel, \
    StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, \
    StateSpecializedPurposeForIncarceration
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import \
    state_incarceration_period

_EMPTY_METADATA = IngestMetadata.new_with_defaults()


class StateIncarcerationPeriodConverterTest(unittest.TestCase):
    """Tests for converting incarcerations."""

    def testParseStateIncarcerationPeriod(self):
        # Arrange
        ingest_incarceration = ingest_info_pb2.StateIncarcerationPeriod(
            status='NOT_IN_CUSTODY',
            incarceration_type='STATE_PRISON',
            facility_security_level='MEDIUM',
            admission_reason='PAROLE_REVOCATION',
            projected_release_reason='CONDITIONAL_RELEASE',
            release_reason='SERVED',
            state_incarceration_period_id='INCARCERATION_ID',
            specialized_purpose_for_incarceration='SHOCK INCARCERATION',
            admission_date='1/2/2111',
            release_date='2/2/2112',
            state_code='us_nd',
            county_code='bis',
            facility='The Prison',
            housing_unit='CB4',
            custodial_authority='STATE_PRISON',
        )

        # Act
        incarceration_builder = entities.StateIncarcerationPeriod.builder()
        state_incarceration_period.copy_fields_to_builder(
            incarceration_builder, ingest_incarceration, _EMPTY_METADATA)
        result = incarceration_builder.build()

        # Assert
        expected_result = entities.StateIncarcerationPeriod(
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text='NOT_IN_CUSTODY',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text='STATE_PRISON',
            facility_security_level=
            StateIncarcerationFacilitySecurityLevel.MEDIUM,
            facility_security_level_raw_text='MEDIUM',
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text='PAROLE_REVOCATION',
            projected_release_reason=
            StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            projected_release_reason_raw_text='CONDITIONAL_RELEASE',
            release_reason=
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            release_reason_raw_text='SERVED',
            specialized_purpose_for_incarceration=
            StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            specialized_purpose_for_incarceration_raw_text=
            'SHOCK INCARCERATION',
            external_id='INCARCERATION_ID',
            admission_date=date(year=2111, month=1, day=2),
            release_date=date(year=2112, month=2, day=2),
            state_code='US_ND',
            county_code='BIS',
            facility='THE PRISON',
            housing_unit='CB4',
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text='STATE_PRISON'
        )

        self.assertEqual(result, expected_result)

    def testParseStateIncarcerationPeriod_temporaryCustody_overwriteAdmission(
            self):
        # Arrange
        ingest_incarceration = ingest_info_pb2.StateIncarcerationPeriod(
            status='NOT_IN_CUSTODY',
            incarceration_type='STATE_PRISON',
            admission_reason='PAROLE_REVOCATION',
            release_reason='RELEASED_FROM_TEMPORARY_CUSTODY',
            admission_date='1/2/2111',
            release_date='2/2/2112',
            state_code='us_nd')

        # Act
        incarceration_builder = entities.StateIncarcerationPeriod.builder()
        state_incarceration_period.copy_fields_to_builder(
            incarceration_builder, ingest_incarceration, _EMPTY_METADATA)
        result = incarceration_builder.build()

        # Assert
        expected_result = entities.StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text='NOT_IN_CUSTODY',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text='STATE_PRISON',
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text='PAROLE_REVOCATION',
            release_reason=
            StateIncarcerationPeriodReleaseReason.
            RELEASED_FROM_TEMPORARY_CUSTODY,
            release_reason_raw_text='RELEASED_FROM_TEMPORARY_CUSTODY',
            admission_date=date(year=2111, month=1, day=2),
            release_date=date(year=2112, month=2, day=2),
            state_code='US_ND')

        self.assertEqual(result, expected_result)

    def testParseStateIncarcerationPeriod_temporaryCustody_leaveAdmission(
            self):
        # Arrange
        ingest_incarceration = ingest_info_pb2.StateIncarcerationPeriod(
            status='NOT_IN_CUSTODY',
            incarceration_type='STATE_PRISON',
            admission_reason='TRANSFER',
            release_reason='RELEASED_FROM_TEMPORARY_CUSTODY',
            admission_date='1/2/2111',
            release_date='2/2/2112',
            state_code='us_nd')

        # Act
        incarceration_builder = entities.StateIncarcerationPeriod.builder()
        state_incarceration_period.copy_fields_to_builder(
            incarceration_builder, ingest_incarceration, _EMPTY_METADATA)
        result = incarceration_builder.build()

        # Assert
        expected_result = entities.StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text='NOT_IN_CUSTODY',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text='STATE_PRISON',
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text='TRANSFER',
            release_reason=
            StateIncarcerationPeriodReleaseReason.
            RELEASED_FROM_TEMPORARY_CUSTODY,
            release_reason_raw_text='RELEASED_FROM_TEMPORARY_CUSTODY',
            admission_date=date(year=2111, month=1, day=2),
            release_date=date(year=2112, month=2, day=2),
            state_code='US_ND')

        self.assertEqual(result, expected_result)

    def testParseStateIncarcerationPeriod_temporaryCustody_overwriteRelease(
            self):
        # Arrange
        ingest_incarceration = ingest_info_pb2.StateIncarcerationPeriod(
            status='NOT_IN_CUSTODY',
            incarceration_type='STATE_PRISON',
            admission_reason='TEMPORARY_CUSTODY',
            release_reason='SERVED',
            admission_date='1/2/2111',
            release_date='2/2/2112',
            state_code='us_nd')

        # Act
        incarceration_builder = entities.StateIncarcerationPeriod.builder()
        state_incarceration_period.copy_fields_to_builder(
            incarceration_builder, ingest_incarceration, _EMPTY_METADATA)
        result = incarceration_builder.build()

        # Assert
        expected_result = entities.StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text='NOT_IN_CUSTODY',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text='STATE_PRISON',
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text='TEMPORARY_CUSTODY',
            release_reason=
            StateIncarcerationPeriodReleaseReason.
            RELEASED_FROM_TEMPORARY_CUSTODY,
            release_reason_raw_text='SERVED',
            admission_date=date(year=2111, month=1, day=2),
            release_date=date(year=2112, month=2, day=2),
            state_code='US_ND')

        self.assertEqual(result, expected_result)

    def testParseStateIncarcerationPeriod_temporaryCustody_leaveRelease(
            self):
        # Arrange
        ingest_incarceration = ingest_info_pb2.StateIncarcerationPeriod(
            status='NOT_IN_CUSTODY',
            incarceration_type='STATE_PRISON',
            admission_reason='TEMPORARY_CUSTODY',
            release_reason='TRANSFER',
            admission_date='1/2/2111',
            release_date='2/2/2112',
            state_code='us_nd')

        # Act
        incarceration_builder = entities.StateIncarcerationPeriod.builder()
        state_incarceration_period.copy_fields_to_builder(
            incarceration_builder, ingest_incarceration, _EMPTY_METADATA)
        result = incarceration_builder.build()

        # Assert
        expected_result = entities.StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text='NOT_IN_CUSTODY',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text='STATE_PRISON',
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text='TEMPORARY_CUSTODY',
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text='TRANSFER',
            admission_date=date(year=2111, month=1, day=2),
            release_date=date(year=2112, month=2, day=2),
            state_code='US_ND')

        self.assertEqual(result, expected_result)

    def testParseStateIncarcerationPeriod_inferStatus_noDates(self):
        # Arrange
        ingest_incarceration = ingest_info_pb2.StateIncarcerationPeriod(
            incarceration_type='STATE_PRISON',
            state_code='us_nd')

        # Act
        incarceration_builder = entities.StateIncarcerationPeriod.builder()
        state_incarceration_period.copy_fields_to_builder(incarceration_builder, ingest_incarceration, _EMPTY_METADATA)
        result = incarceration_builder.build()

        # Assert
        expected_result = entities.StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text='STATE_PRISON',
            state_code='US_ND')

        self.assertEqual(result, expected_result)

    def testParseStateIncarcerationPeriod_inferStatus_releaseDate(self):
        # Arrange
        ingest_incarceration = ingest_info_pb2.StateIncarcerationPeriod(
            incarceration_type='STATE_PRISON',
            admission_date='1/2/2111',
            release_date='1/2/2112',
            state_code='us_nd')

        # Act
        incarceration_builder = entities.StateIncarcerationPeriod.builder()
        state_incarceration_period.copy_fields_to_builder(incarceration_builder, ingest_incarceration, _EMPTY_METADATA)
        result = incarceration_builder.build()

        # Assert
        expected_result = entities.StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text='STATE_PRISON',
            admission_date=date(year=2111, month=1, day=2),
            release_date=date(year=2112, month=1, day=2),
            state_code='US_ND')

        self.assertEqual(result, expected_result)

    def testParseStateIncarcerationPeriod_inferStatus_admissionNoRelease(self):
        # Arrange
        ingest_incarceration = ingest_info_pb2.StateIncarcerationPeriod(
            incarceration_type='STATE_PRISON',
            admission_date='1/2/2111',
            state_code='us_nd')

        # Act
        incarceration_builder = entities.StateIncarcerationPeriod.builder()
        state_incarceration_period.copy_fields_to_builder(incarceration_builder, ingest_incarceration, _EMPTY_METADATA)
        result = incarceration_builder.build()

        # Assert
        expected_result = entities.StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text='STATE_PRISON',
            admission_date=date(year=2111, month=1, day=2),
            state_code='US_ND')

        self.assertEqual(result, expected_result)
