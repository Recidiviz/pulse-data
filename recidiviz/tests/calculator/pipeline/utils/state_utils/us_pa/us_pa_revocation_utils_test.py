# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests the functions in us_pa_revocation_utils.py"""

import unittest
from datetime import date

from recidiviz.calculator.pipeline.utils.state_utils.us_pa import us_pa_revocation_utils
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod, StateSupervisionPeriod

STATE_CODE = 'US_PA'


class TestGetPreRevocationSupervisionType(unittest.TestCase):
    """Tests the us_pa_get_pre_revocation_supervision_type function."""
    def test_us_pa_get_pre_revocation_supervision_type(self):
        revoked_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=STATE_CODE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        supervision_type = us_pa_revocation_utils.us_pa_get_pre_revocation_supervision_type(revoked_supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_type)

    def test_us_pa_get_pre_revocation_supervision_type_none(self):
        revoked_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=STATE_CODE,
            supervision_period_supervision_type=None
        )

        supervision_type = us_pa_revocation_utils.us_pa_get_pre_revocation_supervision_type(revoked_supervision_period)

        self.assertIsNone(supervision_type)

    def test_us_pa_get_pre_revocation_supervision_type_no_revoked_period(self):
        revoked_supervision_period = None

        supervision_type = us_pa_revocation_utils.us_pa_get_pre_revocation_supervision_type(revoked_supervision_period)

        self.assertIsNone(supervision_type)


class TestIsRevocationAdmission(unittest.TestCase):
    """Tests the us_pa_is_revocation_admission function."""

    def test_us_pa_is_revocation_admission_parole(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=STATE_CODE,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION
        )

        self.assertTrue(us_pa_revocation_utils.us_pa_is_revocation_admission(incarceration_period))

    def test_us_pa_is_revocation_admission_probation(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=STATE_CODE,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION
        )

        self.assertTrue(us_pa_revocation_utils.us_pa_is_revocation_admission(incarceration_period))

    def test_us_pa_is_revocation_admission_not_revocation(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=STATE_CODE,
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION
        )

        self.assertFalse(us_pa_revocation_utils.us_pa_is_revocation_admission(incarceration_period))


class TestRevokedSupervisionPeriodsIfRevocationOccurred(unittest.TestCase):
    """Tests the us_pa_revoked_supervision_periods_if_revocation_occurred function."""
    def test_us_pa_revoked_supervision_periods_if_revocation_occurred(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=STATE_CODE,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_date=date(2020, 1, 1)
        )

        revoked_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=STATE_CODE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            start_date=date(2019, 12, 1)
        )

        admission_is_revocation, revoked_supervision_periods = \
            us_pa_revocation_utils.us_pa_revoked_supervision_periods_if_revocation_occurred(
                incarceration_period,
                [revoked_supervision_period])

        self.assertTrue(admission_is_revocation)
        self.assertEqual([revoked_supervision_period], revoked_supervision_periods)

    def test_us_pa_revoked_supervision_periods_if_revocation_occurred_no_revocation(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=STATE_CODE,
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            admission_date=date(2020, 1, 1)
        )

        revoked_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=STATE_CODE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            start_date=date(2019, 12, 1)
        )

        admission_is_revocation, revoked_supervision_periods = \
            us_pa_revocation_utils.us_pa_revoked_supervision_periods_if_revocation_occurred(
                incarceration_period,
                [revoked_supervision_period])

        self.assertFalse(admission_is_revocation)
        self.assertEqual([], revoked_supervision_periods)
