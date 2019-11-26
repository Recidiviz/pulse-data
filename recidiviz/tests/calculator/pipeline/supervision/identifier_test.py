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
# pylint: disable=unused-import,wrong-import-order,protected-access

"""Tests for supervision/identifier.py."""

from datetime import date

import unittest
from freezegun import freeze_time

from recidiviz.calculator.pipeline.supervision import identifier
from recidiviz.calculator.pipeline.supervision.supervision_month import \
    NonRevocationReturnSupervisionMonth, RevocationReturnSupervisionMonth
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason, \
    StateIncarcerationPeriodReleaseReason as ReleaseReason, \
    StateIncarcerationPeriodStatus
from recidiviz.persistence.entity.state.entities import \
    StateSupervisionPeriod, StateIncarcerationPeriod


class TestClassifySupervisionMonths(unittest.TestCase):
    """Tests for the find_supervision_months function."""

    def test_find_supervision_months(self):
        """Tests the find_supervision_months function for a single supervision
        period with no incarceration periods."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_periods = [supervision_period]
        incarceration_periods = []

        supervision_months = identifier.find_supervision_months(
            supervision_periods, incarceration_periods
        )

        assert len(supervision_months) == 3

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 3,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 4,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 5,
                supervision_period.supervision_type)
        ]

    def test_find_supervision_months_overlaps_year(self):
        """Tests the find_supervision_months function for a single supervision
        period with no incarceration periods, where the supervision period
        overlaps two calendar years."""
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2019, 1, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_periods = [supervision_period]
        incarceration_periods = []

        supervision_months = identifier.find_supervision_months(
            supervision_periods, incarceration_periods
        )

        assert len(supervision_months) == 11

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 3,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 4,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 5,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 6,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 7,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 8,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 9,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 10,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 11,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 12,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2019, 1,
                supervision_period.supervision_type)
        ]

    def test_find_supervision_months_two_supervision_periods(self):
        """Tests the find_supervision_months function for two supervision
        periods with no incarceration periods."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2019, 8, 5),
                termination_date=date(2019, 12, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = []

        supervision_months = identifier.find_supervision_months(
            supervision_periods, incarceration_periods
        )

        assert len(supervision_months) == 8

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 3,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 4,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 5,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2019, 8,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2019, 9,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2019, 10,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2019, 11,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2019, 12,
                first_supervision_period.supervision_type)
        ]

    def test_find_supervision_months_overlapping_supervision_periods(self):
        """Tests the find_supervision_months function for two supervision
        periods with no incarceration periods, where the supervision
        periods are of the same type and have overlapping months."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 4, 15),
                termination_date=date(2018, 7, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = []

        supervision_months = identifier.find_supervision_months(
            supervision_periods, incarceration_periods
        )

        assert len(supervision_months) == 7

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 3,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 4,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 5,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 4,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 5,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 6,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 7,
                first_supervision_period.supervision_type)
        ]

    def test_find_supervision_months_overlapping_periods_different_types(self):
        """Tests the find_supervision_months function for two supervision
        periods with no incarceration periods, where the supervision
        periods are of different types and have overlapping months."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 4, 15),
                termination_date=date(2018, 7, 19),
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = []

        supervision_months = identifier.find_supervision_months(
            supervision_periods, incarceration_periods
        )

        assert len(supervision_months) == 7

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 3,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 4,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 5,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                second_supervision_period.state_code, 2018, 4,
                second_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                second_supervision_period.state_code, 2018, 5,
                second_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                second_supervision_period.state_code, 2018, 6,
                second_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                second_supervision_period.state_code, 2018, 7,
                second_supervision_period.supervision_type)
        ]

    def test_find_supervision_months_multiple_periods(self):
        """Tests the find_supervision_months function for two supervision
        periods with two incarceration periods."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2017, 3, 5),
                termination_date=date(2017, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='UT',
                admission_date=date(2017, 5, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2017, 8, 3),
                release_reason=ReleaseReason.SENTENCE_SERVED
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                state_code='UT',
                start_date=date(2018, 8, 5),
                termination_date=date(2018, 12, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=222,
                state_code='UT',
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                admission_date=date(2018, 12, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2019, 3, 3),
                release_reason=ReleaseReason.SENTENCE_SERVED
            )

        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = [first_incarceration_period,
                                 second_incarceration_period]

        supervision_months = identifier.find_supervision_months(
            supervision_periods, incarceration_periods
        )

        assert len(supervision_months) == 8

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2017, 3,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code,
                2017, 4,
                first_supervision_period.supervision_type),
            RevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2017, 5,
                first_supervision_period.supervision_type,
                None, None),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 8,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 9,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 10,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 11,
                first_supervision_period.supervision_type),
            RevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2018, 12,
                first_supervision_period.supervision_type,
                None, None),
        ]

    def test_find_supervision_months_incarceration_overlaps_periods(self):
        """Tests the find_supervision_months function for two supervision
        periods with an incarceration period that overlaps the end of one
        supervision period and the start of another."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2017, 3, 5),
                termination_date=date(2017, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='UT',
                admission_date=date(2017, 5, 15),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2017, 9, 20),
                release_reason=ReleaseReason.SENTENCE_SERVED
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                state_code='UT',
                start_date=date(2017, 8, 5),
                termination_date=date(2017, 12, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = [incarceration_period]

        supervision_months = identifier.find_supervision_months(
            supervision_periods, incarceration_periods
        )

        assert len(supervision_months) == 7

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2017, 3,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code,
                2017, 4,
                first_supervision_period.supervision_type),
            RevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2017, 5,
                first_supervision_period.supervision_type,
                None, None),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2017, 9,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2017, 10,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2017, 11,
                first_supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                first_supervision_period.state_code, 2017, 12,
                first_supervision_period.supervision_type)
        ]

    def test_find_supervision_months_return_next_month(self):
        """Tests the find_supervision_months function
        when there is an incarceration period with a revocation admission
        the month after the supervision period's termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='UT',
                admission_date=date(2018, 6, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]

        supervision_months = identifier.find_supervision_months(
            supervision_periods, incarceration_periods
        )

        assert len(supervision_months) == 4

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 3, supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 4, supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 5, supervision_period.supervision_type),
            RevocationReturnSupervisionMonth(
                incarceration_period.state_code,
                2018, 6, StateSupervisionType.PROBATION, None, None)
        ]

    def test_find_supervision_months_return_months_later(self):
        """Tests the find_supervision_months function
        when there is an incarceration period with a revocation admission
        months after the supervision period's termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='UT',
                admission_date=date(2018, 10, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]

        supervision_months = identifier.find_supervision_months(
            supervision_periods, incarceration_periods
        )

        assert len(supervision_months) == 4

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 3, supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 4, supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 5, supervision_period.supervision_type),
            RevocationReturnSupervisionMonth(
                incarceration_period.state_code,
                2018, 10, StateSupervisionType.PROBATION, None, None)
        ]


class TestFindMonthsForSupervisionPeriod(unittest.TestCase):
    """Tests for the find_months_for_supervision_period function."""

    def test_find_months_for_supervision_period_revocation(self):
        """Tests the find_months_for_supervision_period function
        when there is an incarceration period with a revocation admission
        in the same month as the supervision period's termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 5, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier.identify_months_of_incarceration(
            [incarceration_period]
        )

        supervision_months = identifier.find_months_for_supervision_period(
            supervision_period, indexed_incarceration_periods,
            months_of_incarceration
        )

        assert len(supervision_months) == 3

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 3,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 4,
                supervision_period.supervision_type),
            RevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 5,
                supervision_period.supervision_type,
                None, None)
        ]

    def test_find_months_for_supervision_period_nested_revocation(self):
        """Tests the find_months_for_supervision_period function
        when there is an incarceration period with a revocation admission,
        a stay in prison, and a continued supervision period after release
        from incarceration."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 12, 10),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 5, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2018, 10, 27)
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier.identify_months_of_incarceration(
            [incarceration_period]
        )

        supervision_months = identifier.find_months_for_supervision_period(
            supervision_period, indexed_incarceration_periods,
            months_of_incarceration
        )

        assert len(supervision_months) == 6

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 3,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 4,
                supervision_period.supervision_type),
            RevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 5,
                supervision_period.supervision_type,
                None, None),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 10,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 11,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 12,
                supervision_period.supervision_type)
        ]

    # pylint: disable=line-too-long
    def test_find_months_for_supervision_period_revocation_before_termination(self):
        """Tests the find_months_for_supervision_period function
        when there is an incarceration period with a revocation admission
        before the supervision period's termination_date, but in the same
        month as the termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 5, 3),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier.identify_months_of_incarceration(
            [incarceration_period]
        )

        supervision_months = identifier.find_months_for_supervision_period(
            supervision_period, indexed_incarceration_periods,
            months_of_incarceration
        )

        assert len(supervision_months) == 3

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 3,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 4,
                supervision_period.supervision_type),
            RevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 5,
                supervision_period.supervision_type,
                None, None)
        ]

    # pylint: disable=line-too-long
    def test_find_months_for_supervision_period_revocation_before_termination_month(self):
        """Tests the find_months_for_supervision_period function
        when there is an incarceration period with a revocation admission
        before the supervision period's termination_date, and in a different
        month as the termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 6, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 5, 10),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier.identify_months_of_incarceration(
            [incarceration_period]
        )

        supervision_months = identifier.find_months_for_supervision_period(
            supervision_period, indexed_incarceration_periods,
            months_of_incarceration
        )

        assert len(supervision_months) == 3

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 3,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 4,
                supervision_period.supervision_type),
            RevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 5,
                supervision_period.supervision_type,
                None, None)
        ]

    def test_find_months_for_supervision_period_revocation_no_termination(self):
        """Tests the find_months_for_supervision_period function
        when there is an incarceration period with a revocation admission,
        where there is no termination_date on the supervision_period, and
        no release_date on the incarceration_period."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2003, 7, 5),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2003, 10, 10),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier.identify_months_of_incarceration(
            [incarceration_period]
        )

        supervision_months = identifier.find_months_for_supervision_period(
            supervision_period, indexed_incarceration_periods,
            months_of_incarceration
        )

        assert len(supervision_months) == 4

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2003, 7,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2003, 8,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2003, 9,
                supervision_period.supervision_type),
            RevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2003, 10,
                supervision_period.supervision_type,
                None, None)
        ]

    # pylint: disable=line-too-long
    @freeze_time('2019-11-03')
    def test_find_months_for_supervision_period_nested_revocation_no_termination(self):
        """Tests the find_months_for_supervision_period function
        when there is an incarceration period with a revocation admission,
        a stay in prison, and a continued supervision period after release
        from incarceration that has still not terminated."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2019, 3, 5),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2019, 5, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2019, 10, 17)
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier.identify_months_of_incarceration(
            [incarceration_period]
        )

        supervision_months = identifier.find_months_for_supervision_period(
            supervision_period, indexed_incarceration_periods,
            months_of_incarceration
        )

        assert len(supervision_months) == 5

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2019, 3,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2019, 4,
                supervision_period.supervision_type),
            RevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2019, 5,
                supervision_period.supervision_type,
                None, None),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2019, 10,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2019, 11,
                supervision_period.supervision_type),
        ]

    @freeze_time('2019-09-04')
    def test_find_months_for_supervision_period_admission_today(self):
        """Tests the find_months_for_supervision_period function
        when there is an incarceration period with a revocation admission today,
        where there is no termination_date on the supervision_period, and
        no release_date on the incarceration_period."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2019, 6, 2),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date.today(),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier.identify_months_of_incarceration(
            [incarceration_period]
        )

        supervision_months = identifier.find_months_for_supervision_period(
            supervision_period, indexed_incarceration_periods,
            months_of_incarceration
        )

        assert len(supervision_months) == 4

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2019, 6,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2019, 7,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2019, 8,
                supervision_period.supervision_type),
            RevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2019, 9,
                supervision_period.supervision_type,
                None, None)
        ]

    def test_find_months_for_supervision_period_admission_no_revocation(self):
        """Tests the find_months_for_supervision_period function
        when there is an incarceration period with a non-revocation admission
        before the supervision period's termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 6, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier.identify_months_of_incarceration(
            [incarceration_period]
        )

        supervision_months = identifier.find_months_for_supervision_period(
            supervision_period, indexed_incarceration_periods,
            months_of_incarceration
        )

        assert len(supervision_months) == 4

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 3,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 4,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 5,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 6,
                supervision_period.supervision_type)
        ]

    def test_find_months_for_supervision_period_mismatch_types(self):
        """Tests the find_months_for_supervision_period function
        when there is an incarceration period with a revocation admission
        that does not match the supervision period type."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 6, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 2),
                admission_reason=AdmissionReason.PAROLE_REVOCATION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier.identify_months_of_incarceration(
            [incarceration_period]
        )

        supervision_months = identifier.find_months_for_supervision_period(
            supervision_period, indexed_incarceration_periods,
            months_of_incarceration
        )

        assert len(supervision_months) == 4

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 3,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 4,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 5,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2018, 6,
                supervision_period.supervision_type)
        ]

    def test_find_months_for_supervision_period_ends_on_first(self):
        """Tests the find_months_for_supervision_period function for a
        supervision period with no incarceration periods, where the supervision
        period ends on the 1st day of a month."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2001, 1, 5),
                termination_date=date(2001, 7, 1),
                supervision_type=StateSupervisionType.PROBATION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [])

        months_of_incarceration = identifier.identify_months_of_incarceration(
            []
        )

        supervision_months = identifier.find_months_for_supervision_period(
            supervision_period, indexed_incarceration_periods,
            months_of_incarceration
        )

        assert len(supervision_months) == 7

        assert supervision_months == [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2001, 1, supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2001, 2, supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2001, 3, supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2001, 4, supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2001, 5, supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2001, 6, supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2001, 7, supervision_period.supervision_type)
        ]


class TestIndexIncarcerationPeriodsByAdmissionMonth(unittest.TestCase):
    """Tests the index_incarceration_periods_by_admission_month function."""

    def test_index_incarceration_periods_by_admission_month(self):
        """Tests the index_incarceration_periods_by_admission_month function."""

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 8),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2018, 12, 21)
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period]
            )

        assert indexed_incarceration_periods == {
            2018: {
                6: [incarceration_period]
            }
        }

    def test_index_incarceration_periods_by_admission_month_multiple(self):
        """Tests the index_incarceration_periods_by_admission_month function
        when there are multiple incarceration periods."""

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 8),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2018, 12, 21)
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2019, 3, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [first_incarceration_period, second_incarceration_period]
            )

        assert indexed_incarceration_periods == {
            2018: {
                6: [first_incarceration_period]
            },
            2019: {
                3: [second_incarceration_period]
            }
        }

    # pylint: disable=line-too-long
    def test_index_incarceration_periods_by_admission_month_multiple_month(self):
        """Tests the index_incarceration_periods_by_admission_month function
        when there are multiple incarceration periods with admission dates
        in the same month."""

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 1),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2018, 6, 21)
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 30),
                admission_reason=AdmissionReason.NEW_ADMISSION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [first_incarceration_period, second_incarceration_period]
            )

        assert indexed_incarceration_periods == {
            2018: {
                6: [first_incarceration_period, second_incarceration_period]
            },
        }

    def test_index_incarceration_periods_by_admission_month_none(self):
        """Tests the index_incarceration_periods_by_admission_month function
        when there are no incarceration periods."""
        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month([])

        assert indexed_incarceration_periods == {}


class TestIdentifyMonthsOfIncarceration(unittest.TestCase):
    """Tests the identify_months_of_incarceration function."""

    def test_identify_months_of_incarceration_incarcerated(self):
        """Tests the identify_months_of_incarceration function."""
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 8),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2018, 12, 21)
            )

        months_incarcerated = \
            identifier.identify_months_of_incarceration([incarceration_period])

        assert months_incarcerated == {
            (2018, 7), (2018, 8), (2018, 9), (2018, 10), (2018, 11)
        }

    def test_identify_months_of_incarceration_incarcerated_on_first(self):
        """Tests the identify_months_of_incarceration function where the person
        was incarcerated on the first of the month."""
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 8, 1),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2018, 12, 21)
            )

        months_incarcerated = \
            identifier.identify_months_of_incarceration([incarceration_period])

        assert months_incarcerated == {
            (2018, 8), (2018, 9), (2018, 10), (2018, 11)
        }

    def test_identify_months_of_incarceration_released_last_day(self):
        """Tests the identify_months_of_incarceration function where the person
        was released on the last day of a month."""
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 8, 15),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2018, 10, 31)
            )

        months_incarcerated = \
            identifier.identify_months_of_incarceration([incarceration_period])

        assert months_incarcerated == {
            (2018, 9), (2018, 10)
        }

    def test_identify_months_of_incarceration_no_full_months(self):
        """Tests the identify_months_of_incarceration function where the person
        was not incarcerated for a full month."""
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2013, 3, 1),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2013, 3, 30)
            )

        months_incarcerated = \
            identifier.identify_months_of_incarceration([incarceration_period])

        assert months_incarcerated == set()

    def test_identify_months_of_incarceration_leap_year(self):
        """Tests the identify_months_of_incarceration function where the person
        was incarcerated until the 28th of February during a leap year, so they
        were not incarcerated for a full month."""
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(1996, 2, 1),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(1996, 2, 28)
            )

        months_incarcerated = \
            identifier.identify_months_of_incarceration([incarceration_period])

        assert months_incarcerated == set()
