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
"""Tests for us_nd_supervision_type_identification.py"""
import unittest
from datetime import date

import pytest

from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_type_identification import \
    us_nd_get_post_incarceration_supervision_type
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodStatus, \
    StateIncarcerationPeriodAdmissionReason, StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class TestUsNdSupervisionTypeIdentification(unittest.TestCase):
    """Tests the us_nd_get_post_incarceration_supervision_type function."""
    def test_us_nd_get_post_incarceration_supervision_type_parole(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id='2',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            facility='PRISON',
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text='Revocation',
            release_date=date(2008, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE)

        parole_raw_text_values = ['RPAR', 'PARL', 'PV']

        for value in parole_raw_text_values:
            incarceration_period.release_reason_raw_text = value
            supervision_type_at_release = us_nd_get_post_incarceration_supervision_type(incarceration_period)

            self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_type_at_release)

    def test_us_nd_get_post_incarceration_supervision_type_probation(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id='2',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            facility='PRISON',
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text='Revocation',
            release_date=date(2008, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE)

        parole_raw_text_values = ['RPRB', 'NPROB', 'NPRB', 'PRB']

        for value in parole_raw_text_values:
            incarceration_period.release_reason_raw_text = value
            supervision_type_at_release = us_nd_get_post_incarceration_supervision_type(incarceration_period)

            self.assertEqual(StateSupervisionPeriodSupervisionType.PROBATION, supervision_type_at_release)

    def test_us_nd_get_post_incarceration_supervision_type_no_supervision(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id='2',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            facility='PRISON',
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text='Revocation',
            release_date=date(2008, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            release_reason_raw_text='X')

        supervision_type_at_release = us_nd_get_post_incarceration_supervision_type(incarceration_period)

        self.assertIsNone(supervision_type_at_release)

    def test_us_nd_get_post_incarceration_supervision_type_unexpected_raw_text(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id='2',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_ND',
            facility='PRISON',
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text='Revocation',
            release_date=date(2008, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text='NOT A VALID RAW TEXT VALUE')

        with pytest.raises(ValueError):
            _ = us_nd_get_post_incarceration_supervision_type(incarceration_period)
