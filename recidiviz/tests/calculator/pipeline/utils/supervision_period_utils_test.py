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
# pylint: disable=unused-import,wrong-import-order

"""Tests for supervision_period_utils.py."""
import unittest
from datetime import date

from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.calculator.pipeline.utils.supervision_period_utils import \
    find_last_supervision_period_terminated_before_date, SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT, \
    prepare_supervision_periods_for_calculations
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType, \
    StateSupervisionPeriodTerminationReason, StateSupervisionPeriodStatus, StateSupervisionPeriodAdmissionReason
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod


class TestFindMostRecentlyTerminatedSupervisionPeriod(unittest.TestCase):
    """Tests the _find_most_recently_terminated_supervision_period function."""

    def test_find_most_recently_terminated_supervision_period(self):
        supervision_period_older = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2000, 1, 1),
            termination_date=date(2005, 1, 1)
        )

        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2006, 3, 1),
            termination_date=date(2010, 9, 1)
        )

        most_recently_terminated_period = find_last_supervision_period_terminated_before_date(
            upper_bound_date=date(2010, 10, 1),
            supervision_periods=[supervision_period_older, supervision_period_recent])

        self.assertEqual(supervision_period_recent, most_recently_terminated_period)

    def test_find_most_recently_terminated_supervision_period_none(self):
        supervision_period_older = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2000, 1, 1),
            termination_date=date(2005, 1, 1)
        )

        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2006, 3, 1),
            termination_date=date(2010, 1, 1)
        )

        most_recently_terminated_period = find_last_supervision_period_terminated_before_date(
            upper_bound_date=date(1990, 10, 1),
            supervision_periods=[supervision_period_older, supervision_period_recent])

        self.assertIsNone(most_recently_terminated_period)

    def test_find_most_recently_terminated_supervision_period_ends_before_cutoff(self):
        supervision_period_older = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2000, 1, 1),
            termination_date=date(2005, 1, 1)
        )

        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2006, 3, 1),
            termination_date=date(2010, 1, 1)
        )

        # Set the admission date to be 1 day after the cut-off for how close a supervision period termination has to be
        # to a revocation admission to be counted as a proximal supervision period
        admission_date = (supervision_period_recent.termination_date +
                          relativedelta(months=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT)
                          + relativedelta(days=1))

        most_recently_terminated_period = find_last_supervision_period_terminated_before_date(
            upper_bound_date=admission_date,
            supervision_periods=[supervision_period_older, supervision_period_recent])

        self.assertIsNone(most_recently_terminated_period)

    def test_find_most_recently_terminated_supervision_period_ends_on_cutoff_date(self):
        supervision_period_older = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2000, 1, 1),
            termination_date=date(2005, 1, 1)
        )

        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2006, 3, 1),
            termination_date=date(2010, 1, 1)
        )

        # Set the admission date to be on the last day of the cut-off for how close a supervision period termination
        # has to be to a revocation admission to be counted as a proximal supervision period
        admission_date = (supervision_period_recent.termination_date +
                          relativedelta(days=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT))

        most_recently_terminated_period = find_last_supervision_period_terminated_before_date(
            upper_bound_date=admission_date,
            supervision_periods=[supervision_period_older, supervision_period_recent])

        self.assertEqual(supervision_period_recent, most_recently_terminated_period)

    def test_find_most_recently_terminated_supervision_period_overlapping(self):
        supervision_period_older = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2000, 1, 1),
            termination_date=date(2007, 9, 20)
        )

        # Overlapping supervision period should not be returned
        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2006, 3, 1),
            termination_date=date(2010, 1, 1)
        )

        most_recently_terminated_period = find_last_supervision_period_terminated_before_date(
            upper_bound_date=date(2007, 10, 1),
            supervision_periods=[supervision_period_older, supervision_period_recent])

        self.assertEqual(supervision_period_older, most_recently_terminated_period)

    def test_find_most_recently_terminated_supervision_period_overlapping_no_termination(self):
        supervision_period_older = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2000, 1, 1),
            termination_date=date(2007, 9, 20)
        )

        # Overlapping supervision period that should have been found in the other identifier code
        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2006, 3, 1),
        )

        most_recently_terminated_period = find_last_supervision_period_terminated_before_date(
            upper_bound_date=date(2007, 10, 1),
            supervision_periods=[supervision_period_older, supervision_period_recent])

        self.assertEqual(supervision_period_older, most_recently_terminated_period)

    def test_find_most_recently_terminated_supervision_period_no_periods(self):
        most_recently_terminated_period = find_last_supervision_period_terminated_before_date(
            upper_bound_date=date(1990, 10, 1),
            supervision_periods=[])

        self.assertIsNone(most_recently_terminated_period)

    def test_find_most_recently_terminated_supervision_period_month_boundary(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2000, 1, 1),
            termination_date=date(2005, 3, 31)
        )

        most_recently_terminated_period = find_last_supervision_period_terminated_before_date(
            upper_bound_date=date(2005, 4, 1),
            supervision_periods=[supervision_period])

        self.assertEqual(supervision_period, most_recently_terminated_period)

    def test_find_most_recently_terminated_supervision_period_ends_on_admission_date(self):
        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2006, 3, 1),
            termination_date=date(2007, 12, 31)
        )

        most_recently_terminated_period = find_last_supervision_period_terminated_before_date(
            upper_bound_date=date(2007, 12, 31),
            supervision_periods=[supervision_period_recent])

        self.assertEqual(supervision_period_recent, most_recently_terminated_period)

    def test_find_most_recently_terminated_supervision_period_starts_on_admission_date(self):
        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX',
            start_date=date(2006, 1, 1),
            termination_date=date(2007, 12, 31)
        )

        most_recently_terminated_period = find_last_supervision_period_terminated_before_date(
            upper_bound_date=date(2006, 1, 1),
            supervision_periods=[supervision_period_recent])

        self.assertIsNone(most_recently_terminated_period)


class TestPrepareSupervisionPeriodsForCalculations(unittest.TestCase):
    """Tests the prepare_supervision_periods_for_calculations function."""
    def test_prepare_supervision_periods_for_calculations(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ND',
            start_date=date(2006, 1, 1),
            termination_date=date(2007, 12, 31)
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period], drop_federal_and_other_country_supervision_periods=False)

        self.assertEqual([supervision_period], updated_periods)

    @freeze_time('2000-01-01')
    def test_prepare_supervision_periods_for_calculations_drop_future_dates(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ND',
            start_date=date(2006, 1, 1),
            termination_date=date(2007, 12, 31)
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period], drop_federal_and_other_country_supervision_periods=False)

        self.assertEqual([], updated_periods)

    @freeze_time('2000-01-01')
    def test_prepare_supervision_periods_for_calculations_unset_future_release_dates(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
            state_code='US_ND',
            start_date=date(1990, 1, 1),
            termination_date=date(2007, 12, 31),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period], drop_federal_and_other_country_supervision_periods=False)

        updated_period = StateSupervisionPeriod.new_with_defaults(
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
            state_code='US_ND',
            start_date=date(1990, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            termination_date=None,
            termination_reason=None
        )

        self.assertEqual([updated_period], updated_periods)

    def test_prepare_supervision_periods_for_calculations_placeholder(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code='US_XX',
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period], drop_federal_and_other_country_supervision_periods=False)
        self.assertEqual([], updated_periods)

    def test_prepare_supervision_periods_for_calculations_usID_drop_federal(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            start_date=date(2006, 1, 1),
            termination_date=date(2007, 12, 31),
            custodial_authority=StateCustodialAuthority.FEDERAL,  # Not the state's authority
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period], drop_federal_and_other_country_supervision_periods=True)

        self.assertEqual([], updated_periods)

    def test_prepare_supervision_periods_for_calculations_usID_drop_other_country(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            start_date=date(2006, 1, 1),
            termination_date=date(2007, 12, 31),
            custodial_authority=StateCustodialAuthority.OTHER_COUNTRY,  # Not the state's authority
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period], drop_federal_and_other_country_supervision_periods=True)

        self.assertEqual([], updated_periods)
