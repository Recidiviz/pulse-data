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

"""Tests for period_utils.py."""
import unittest
from datetime import date

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.period_utils import (
    find_last_terminated_period_before_date,
    find_earliest_date_of_period_ending_in_death,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionPeriod,
    StateIncarcerationPeriod,
)


class TestLastTerminatedPeriodBeforeDate(unittest.TestCase):
    """Tests the find_last_terminated_period_before_date function."""

    def test_find_last_terminated_period_before_date(self):
        supervision_period_older = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2000, 1, 1),
            termination_date=date(2010, 8, 30),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2006, 3, 1),
            termination_date=date(2010, 9, 1),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        most_recently_terminated_period = find_last_terminated_period_before_date(
            upper_bound_date=date(2010, 10, 1),
            periods=[
                supervision_period_older,
                supervision_period_recent,
            ],
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        self.assertEqual(supervision_period_recent, most_recently_terminated_period)

    def test_find_last_terminated_period_before_date_none(self):
        supervision_period_older = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2000, 1, 1),
            termination_date=date(2005, 1, 1),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2006, 3, 1),
            termination_date=date(2010, 1, 1),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        most_recently_terminated_period = find_last_terminated_period_before_date(
            upper_bound_date=date(1990, 10, 1),
            periods=[
                supervision_period_older,
                supervision_period_recent,
            ],
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        self.assertIsNone(most_recently_terminated_period)

    def test_find_last_terminated_period_before_date_ends_before_cutoff(self):
        supervision_period_older = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2000, 1, 1),
            termination_date=date(2005, 1, 1),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2006, 3, 1),
            termination_date=date(2010, 1, 1),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # Set the admission date to be 1 day after the cut-off for how close a supervision period termination has to be
        # to a revocation admission to be counted as a proximal supervision period
        admission_date = (
            supervision_period_recent.termination_date
            + relativedelta(months=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT)
            + relativedelta(days=1)
        )

        most_recently_terminated_period = find_last_terminated_period_before_date(
            upper_bound_date=admission_date,
            periods=[
                supervision_period_older,
                supervision_period_recent,
            ],
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        self.assertIsNone(most_recently_terminated_period)

    def test_find_last_terminated_period_before_date_ends_on_cutoff_date(self):
        supervision_period_older = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2000, 1, 1),
            termination_date=date(2005, 1, 1),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2006, 3, 1),
            termination_date=date(2010, 1, 1),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # Set the admission date to be on the last day of the cut-off for how close a supervision period termination
        # has to be to a revocation admission to be counted as a proximal supervision period
        admission_date = supervision_period_recent.termination_date + relativedelta(
            days=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT
        )

        most_recently_terminated_period = find_last_terminated_period_before_date(
            upper_bound_date=admission_date,
            periods=[
                supervision_period_older,
                supervision_period_recent,
            ],
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        self.assertEqual(supervision_period_recent, most_recently_terminated_period)

    def test_find_last_terminated_period_before_date_overlapping(self):
        supervision_period_older = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2000, 1, 1),
            termination_date=date(2007, 9, 20),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # Overlapping supervision period should not be returned
        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2006, 3, 1),
            termination_date=date(2010, 1, 1),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        most_recently_terminated_period = find_last_terminated_period_before_date(
            upper_bound_date=date(2007, 10, 1),
            periods=[
                supervision_period_older,
                supervision_period_recent,
            ],
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        self.assertEqual(supervision_period_older, most_recently_terminated_period)

    def test_find_last_terminated_period_before_date_overlapping_no_termination(
        self,
    ):
        supervision_period_older = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2000, 1, 1),
            termination_date=date(2007, 9, 20),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # Overlapping supervision period that should have been found in the other identifier code
        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2006, 3, 1),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        most_recently_terminated_period = find_last_terminated_period_before_date(
            upper_bound_date=date(2007, 10, 1),
            periods=[
                supervision_period_older,
                supervision_period_recent,
            ],
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        self.assertEqual(supervision_period_older, most_recently_terminated_period)

    def test_find_last_terminated_period_before_date_no_periods(self):
        most_recently_terminated_period = find_last_terminated_period_before_date(
            upper_bound_date=date(1990, 10, 1),
            periods=[],
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        self.assertIsNone(most_recently_terminated_period)

    def test_find_last_terminated_period_before_date_month_boundary(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2000, 1, 1),
            termination_date=date(2005, 3, 31),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        most_recently_terminated_period = find_last_terminated_period_before_date(
            upper_bound_date=date(2005, 4, 1),
            periods=[supervision_period],
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        self.assertEqual(supervision_period, most_recently_terminated_period)

    def test_find_last_terminated_period_before_date_ends_on_admission_date(
        self,
    ):
        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2006, 3, 1),
            termination_date=date(2007, 12, 31),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        most_recently_terminated_period = find_last_terminated_period_before_date(
            upper_bound_date=date(2007, 12, 31),
            periods=[supervision_period_recent],
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        self.assertEqual(supervision_period_recent, most_recently_terminated_period)

    def test_find_last_terminated_period_before_date_starts_on_admission_date(
        self,
    ):
        supervision_period_recent = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2006, 1, 1),
            termination_date=date(2007, 12, 31),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        most_recently_terminated_period = find_last_terminated_period_before_date(
            upper_bound_date=date(2006, 1, 1),
            periods=[supervision_period_recent],
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        self.assertIsNone(most_recently_terminated_period)

    def test_find_last_terminated_period_before_date_same_termination_date(
        self,
    ):
        supervision_period_a = StateSupervisionPeriod.new_with_defaults(
            external_id="a",
            state_code="US_XX",
            start_date=date(2006, 1, 1),
            termination_date=date(2007, 12, 31),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_b = StateSupervisionPeriod.new_with_defaults(
            external_id="b",
            state_code="US_XX",
            start_date=date(2006, 1, 1),
            termination_date=date(2007, 12, 31),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        most_recently_terminated_period = find_last_terminated_period_before_date(
            upper_bound_date=date(2008, 1, 10),
            periods=[supervision_period_a, supervision_period_b],
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        self.assertEqual(supervision_period_a, most_recently_terminated_period)

    def test_find_last_terminated_period_before_date_same_termination_date_override(
        self,
    ):
        """Tests the find_last_terminated_period_before_date function when a
        same_date_sort_fn function is included.
        """
        supervision_period_a = StateSupervisionPeriod.new_with_defaults(
            external_id="a",
            state_code="US_XX",
            start_date=date(2006, 1, 1),
            termination_date=date(2007, 12, 31),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )

        supervision_period_b = StateSupervisionPeriod.new_with_defaults(
            external_id="b",
            state_code="US_XX",
            start_date=date(2006, 1, 1),
            termination_date=date(2007, 12, 31),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
        )

        def _same_date_sort_override(
            period_a: StateSupervisionPeriod, period_b: StateSupervisionPeriod
        ) -> int:
            prioritized_termination_reasons = [
                StateSupervisionPeriodTerminationReason.ABSCONSION,
                StateSupervisionPeriodTerminationReason.EXPIRATION,
            ]
            prioritize_a = (
                period_a.termination_reason in prioritized_termination_reasons
            )
            prioritize_b = (
                period_b.termination_reason in prioritized_termination_reasons
            )

            if prioritize_a and prioritize_b:
                return 0
            return -1 if prioritize_a else 1

        most_recently_terminated_period = find_last_terminated_period_before_date(
            upper_bound_date=date(2008, 1, 10),
            periods=[supervision_period_a, supervision_period_b],
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
            same_date_sort_fn=_same_date_sort_override,
        )

        self.assertEqual(supervision_period_b, most_recently_terminated_period)


class TestEarliestPeriodEndingInDeath(unittest.TestCase):
    """Tests the find_earliest_period_ending_in_death function."""

    def test_find_earliest_period_ending_in_death_in_sp(self):
        supervision_period_death = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2000, 1, 1),
            termination_date=date(2005, 1, 2),
            termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            admission_date=date(2005, 1, 1),
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
        )

        earliest_date_of_period_ending_in_death = (
            find_earliest_date_of_period_ending_in_death(
                periods=[
                    supervision_period_death,
                    incarceration_period,
                ],
            )
        )

        self.assertEqual(
            supervision_period_death.end_date_exclusive,
            earliest_date_of_period_ending_in_death,
        )

    def test_find_earliest_period_ending_in_death_multiple_ip_death(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2000, 1, 1),
            termination_date=date(2005, 1, 2),
            status=StateSupervisionPeriodStatus.TERMINATED,
        )

        later_death_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            admission_date=date(2005, 1, 3),
            release_date=date(2005, 1, 6),
            release_reason=StateIncarcerationPeriodReleaseReason.DEATH,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
        )

        earliest_death_incarceration_period = (
            StateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                admission_date=date(2005, 1, 3),
                release_date=date(2005, 1, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.DEATH,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            )
        )

        earliest_date_of_period_ending_in_death = (
            find_earliest_date_of_period_ending_in_death(
                periods=[
                    supervision_period,
                    later_death_incarceration_period,
                    earliest_death_incarceration_period,
                ],
            )
        )

        self.assertEqual(
            earliest_death_incarceration_period.end_date_exclusive,
            earliest_date_of_period_ending_in_death,
        )

    def test_find_earliest_period_ending_in_death_no_deaths(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2000, 1, 1),
            termination_date=date(2005, 1, 2),
            status=StateSupervisionPeriodStatus.TERMINATED,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            admission_date=date(2005, 1, 3),
            release_date=date(2005, 1, 6),
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
        )

        earliest_date_of_period_ending_in_death = (
            find_earliest_date_of_period_ending_in_death(
                periods=[
                    supervision_period,
                    incarceration_period,
                ],
            )
        )

        self.assertIsNone(earliest_date_of_period_ending_in_death)

    def test_find_earliest_period_ending_in_death_both_sp_ip_deaths(self):
        supervision_period_death = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2000, 1, 1),
            termination_date=date(2005, 1, 4),
            termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
            status=StateSupervisionPeriodStatus.TERMINATED,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            admission_date=date(2003, 1, 3),
            release_date=date(2004, 1, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
        )

        incarceration_period_death = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            admission_date=date(2004, 1, 3),
            release_date=date(2005, 1, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.DEATH,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
        )

        earliest_date_of_period_ending_in_death = (
            find_earliest_date_of_period_ending_in_death(
                periods=[
                    supervision_period_death,
                    incarceration_period,
                    incarceration_period_death,
                ],
            )
        )

        self.assertEqual(
            incarceration_period_death.end_date_exclusive,
            earliest_date_of_period_ending_in_death,
        )

    def test_find_earliest_period_ending_in_death_no_end_date(self):
        supervision_period_death = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            start_date=date(2000, 1, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
            status=StateSupervisionPeriodStatus.TERMINATED,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            admission_date=date(2003, 1, 3),
            release_date=date(2004, 1, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
        )

        incarceration_period_death = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            admission_date=date(2004, 1, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.DEATH,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
        )

        earliest_period_ending_in_death = find_earliest_date_of_period_ending_in_death(
            periods=[
                supervision_period_death,
                incarceration_period,
                incarceration_period_death,
            ],
        )

        self.assertIsNone(earliest_period_ending_in_death)
