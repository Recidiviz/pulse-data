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
"""Tests for incarceration_period_index.py."""

import unittest
from datetime import date, timedelta
from typing import Dict, List, Optional

from freezegun import freeze_time

from recidiviz.calculator.pipeline.utils.pre_processed_incarceration_period_index import (
    PreProcessedIncarcerationPeriodIndex,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason as AdmissionReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason as ReleaseReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.date import DateRange, date_or_tomorrow
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


def _get_incarceration_period_index(
    incarceration_periods: List[StateIncarcerationPeriod],
    ip_id_to_pfi_subtype: Optional[Dict[int, Optional[str]]] = None,
) -> PreProcessedIncarcerationPeriodIndex:
    """Helper function for instantiating an PreProcessedIncarcerationPeriodIndex."""
    return PreProcessedIncarcerationPeriodIndex(
        incarceration_periods=incarceration_periods,
        ip_id_to_pfi_subtype=(
            ip_id_to_pfi_subtype
            or {
                ip.incarceration_period_id: None
                for ip in incarceration_periods
                if ip.incarceration_period_id
            }
        ),
    )


class TestIndexIncarcerationPeriodsByAdmissionMonth(unittest.TestCase):
    """Tests the index_incarceration_periods_by_admission_date function."""

    def test_index_incarceration_periods_by_admission_date(self):
        """Tests the index_incarceration_periods_by_admission_date function."""

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 6, 8),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 12, 21),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_index = _get_incarceration_period_index(
            [incarceration_period]
        )

        self.assertEqual(
            incarceration_period_index.incarceration_periods_by_admission_date,
            {incarceration_period.admission_date: [incarceration_period]},
        )

    def test_index_incarceration_periods_by_admission_date_multiple(self):
        """Tests the index_incarceration_periods_by_admission_date function
        when there are multiple incarceration periods."""

        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 6, 8),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 12, 21),
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_period_id=111,
            external_id="ip2",
            state_code="US_XX",
            admission_date=date(2019, 3, 2),
            admission_reason=AdmissionReason.NEW_ADMISSION,
        )

        incarceration_period_index = _get_incarceration_period_index(
            [first_incarceration_period, second_incarceration_period]
        )

        self.assertEqual(
            incarceration_period_index.incarceration_periods_by_admission_date,
            {
                first_incarceration_period.admission_date: [first_incarceration_period],
                second_incarceration_period.admission_date: [
                    second_incarceration_period
                ],
            },
        )

    def test_index_incarceration_periods_by_admission_date_multiple_in_day(self):
        """Tests the index_incarceration_periods_by_admission_date function when there are multiple incarceration
        periods with the same admission dates."""

        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 6, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 6, 1),
            release_reason=ReleaseReason.TRANSFER,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_XX",
            admission_date=date(2018, 6, 1),
            admission_reason=AdmissionReason.TRANSFER,
        )

        incarceration_period_index = _get_incarceration_period_index(
            [first_incarceration_period, second_incarceration_period]
        )

        self.assertEqual(
            {
                first_incarceration_period.admission_date: [
                    first_incarceration_period,
                    second_incarceration_period,
                ]
            },
            incarceration_period_index.incarceration_periods_by_admission_date,
        )

    def test_index_incarceration_periods_by_admission_date_none(self):
        """Tests the index_incarceration_periods_by_admission_date function
        when there are no incarceration periods."""
        incarceration_period_index = _get_incarceration_period_index([])

        self.assertEqual(
            incarceration_period_index.incarceration_periods_by_admission_date, {}
        )


class TestMonthsExcludedFromSupervisionPopulation(unittest.TestCase):
    """Tests the months_excluded_from_supervision_population function."""

    def test_months_excluded_from_supervision_population_incarcerated(self):
        """Tests the months_excluded_from_supervision_population function."""
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 6, 8),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 12, 21),
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_index = _get_incarceration_period_index(
            [incarceration_period]
        )

        self.assertEqual(
            incarceration_period_index.months_excluded_from_supervision_population,
            {(2018, 7), (2018, 8), (2018, 9), (2018, 10), (2018, 11)},
        )

    def test_months_excluded_from_supervision_population_incarcerated_on_first(self):
        """Tests the months_excluded_from_supervision_population function where the person
        was incarcerated on the first of the month."""
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 8, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 12, 21),
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_index = _get_incarceration_period_index(
            [incarceration_period]
        )

        self.assertEqual(
            incarceration_period_index.months_excluded_from_supervision_population,
            {(2018, 8), (2018, 9), (2018, 10), (2018, 11)},
        )

    def test_months_excluded_from_supervision_population_released_last_day(self):
        """Tests the months_excluded_from_supervision_population function where the person
        was released on the last day of a month."""
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 8, 15),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 10, 31),
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_index = _get_incarceration_period_index(
            [incarceration_period]
        )

        self.assertEqual(
            incarceration_period_index.months_excluded_from_supervision_population,
            {
                (2018, 9)
                # The person is not counted as incarcerated on 10/31/2018, so they are not fully incarcerated this month
            },
        )

    def test_months_excluded_from_supervision_population_no_full_months(self):
        """Tests the months_excluded_from_supervision_population function where the person
        was not incarcerated for a full month."""
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2013, 3, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2013, 3, 30),
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_index = _get_incarceration_period_index(
            [incarceration_period]
        )

        self.assertEqual(
            incarceration_period_index.months_excluded_from_supervision_population,
            set(),
        )

    def test_months_excluded_from_supervision_population_leap_year(self):
        """Tests the months_excluded_from_supervision_population function where the person
        was incarcerated until the 28th of February during a leap year, so they
        were not incarcerated for a full month."""
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(1996, 2, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(1996, 2, 28),
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_index = _get_incarceration_period_index(
            [incarceration_period]
        )

        self.assertEqual(
            incarceration_period_index.months_excluded_from_supervision_population,
            set(),
        )

    def test_identify_months_fully_incarcerated_two_consecutive_periods(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2005, 3, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2005, 3, 15),
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_index = _get_incarceration_period_index(
            [incarceration_period]
        )

        self.assertEqual(
            incarceration_period_index.months_excluded_from_supervision_population,
            set(),
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_XX",
            admission_date=date(2005, 3, 15),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2005, 4, 2),
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_index = _get_incarceration_period_index(
            [incarceration_period, incarceration_period_2]
        )

        self.assertEqual(
            incarceration_period_index.months_excluded_from_supervision_population,
            {(2005, 3)},
        )

    def test_identify_months_fully_incarcerated_two_consecutive_periods_do_not_cover(
        self,
    ):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2005, 3, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2005, 3, 15),
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_index = _get_incarceration_period_index(
            [incarceration_period]
        )

        self.assertEqual(
            incarceration_period_index.months_excluded_from_supervision_population,
            set(),
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_XX",
            admission_date=date(2005, 3, 15),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2005, 3, 20),
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_index = _get_incarceration_period_index(
            [incarceration_period, incarceration_period_2]
        )

        self.assertEqual(
            incarceration_period_index.months_excluded_from_supervision_population,
            set(),
        )


class TestIndexMonthToOverlappingIPsNotUnderSupervisionAuthority(unittest.TestCase):
    """Tests the month_to_overlapping_ips_not_under_supervision_authority initialization function."""

    def test_no_periods(self):
        index = _get_incarceration_period_index([])
        self.assertEqual(
            index.month_to_overlapping_ips_not_under_supervision_authority, {}
        )

    def test_one_period_start_end_middle_of_months(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=444,
            external_id="ip4",
            state_code="US_XX",
            admission_date=date(2007, 12, 2),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 3, 28),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        index = _get_incarceration_period_index([incarceration_period])

        expected = {
            2007: {12: [incarceration_period]},
            2008: {
                1: [incarceration_period],
                2: [incarceration_period],
                3: [incarceration_period],
            },
        }

        self.assertEqual(
            index.month_to_overlapping_ips_not_under_supervision_authority, expected
        )

    def test_one_period_start_end_exactly_on_month(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=444,
            external_id="ip4",
            state_code="US_XX",
            admission_date=date(2007, 12, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 2, 1),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        index = _get_incarceration_period_index([incarceration_period])

        expected = {
            2007: {12: [incarceration_period]},
            2008: {1: [incarceration_period]},
        }

        self.assertEqual(
            index.month_to_overlapping_ips_not_under_supervision_authority, expected
        )

    @freeze_time("2008-04-01")
    def test_period_no_termination(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=444,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id="ip4",
            state_code="US_XX",
            admission_date=date(2007, 12, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        index = _get_incarceration_period_index([incarceration_period])

        expected = {
            2007: {12: [incarceration_period]},
            2008: {
                1: [incarceration_period],
                2: [incarceration_period],
                3: [incarceration_period],
                4: [incarceration_period],
            },
        }

        self.assertEqual(
            index.month_to_overlapping_ips_not_under_supervision_authority, expected
        )

    @freeze_time("2008-04-01")
    def test_period_no_release_date_not_in_custody(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=444,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="ip4",
            state_code="US_XX",
            admission_date=date(2007, 12, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        with self.assertRaises(ValueError):
            _ = _get_incarceration_period_index([incarceration_period])

    def test_multiple_periods(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=444,
            external_id="ip4",
            state_code="US_XX",
            admission_date=date(2007, 12, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 2, 2),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=555,
            external_id="ip5",
            state_code="US_XX",
            admission_date=date(2008, 2, 4),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 4, 5),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        index = _get_incarceration_period_index(
            [incarceration_period, incarceration_period_2]
        )

        expected = {
            2007: {12: [incarceration_period]},
            2008: {
                1: [incarceration_period],
                2: [incarceration_period, incarceration_period_2],
                3: [incarceration_period_2],
                4: [incarceration_period_2],
            },
        }

        self.assertEqual(
            index.month_to_overlapping_ips_not_under_supervision_authority, expected
        )

    def test_period_starts_ends_same_month(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=444,
            external_id="ip4",
            state_code="US_XX",
            admission_date=date(2008, 2, 4),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 2, 5),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        index = _get_incarceration_period_index([incarceration_period])

        expected = {
            2008: {
                2: [incarceration_period],
            },
        }

        self.assertEqual(
            index.month_to_overlapping_ips_not_under_supervision_authority, expected
        )

    def test_multiple_periods_one_under_supervision_authority(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=444,
            external_id="ip4",
            state_code="US_XX",
            admission_date=date(2007, 12, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 2, 2),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # This period has a supervision custodial authority
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=555,
            external_id="ip5",
            state_code="US_XX",
            admission_date=date(2008, 2, 4),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 4, 5),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        index = _get_incarceration_period_index(
            [incarceration_period, incarceration_period_2]
        )

        expected = {
            2007: {12: [incarceration_period]},
            2008: {1: [incarceration_period], 2: [incarceration_period]},
        }

        self.assertEqual(
            index.month_to_overlapping_ips_not_under_supervision_authority, expected
        )


class TesIsExcludedFromSupervisionPopulationForRange(unittest.TestCase):
    """Tests the is_excluded_from_supervision_population_for_range function."""

    def setUp(self) -> None:
        incarceration_period_partial_month = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2007, 2, 5),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2007, 2, 8),
            release_reason=ReleaseReason.TRANSFER,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_partial_month_2 = (
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=222,
                external_id="ip2",
                state_code="US_XX",
                admission_date=date(2007, 2, 8),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2007, 2, 15),
                release_reason=ReleaseReason.TRANSFER,
                status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
            )
        )

        incarceration_period_partial_month_3 = (
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=333,
                external_id="ip3",
                state_code="US_XX",
                admission_date=date(2007, 2, 15),
                admission_reason=AdmissionReason.TRANSFER,
                release_date=date(2007, 2, 20),
                release_reason=ReleaseReason.SENTENCE_SERVED,
                status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
            )
        )

        incarceration_period_mulitple_months = (
            StateIncarcerationPeriod.new_with_defaults(
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                incarceration_period_id=444,
                external_id="ip4",
                state_code="US_XX",
                admission_date=date(2008, 1, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 5, 28),
                release_reason=ReleaseReason.SENTENCE_SERVED,
            )
        )

        incarceration_period_mulitple_months_2 = (
            StateIncarcerationPeriod.new_with_defaults(
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                incarceration_period_id=555,
                external_id="ip5",
                state_code="US_XX",
                admission_date=date(2008, 5, 28),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 7, 15),
                release_reason=ReleaseReason.SENTENCE_SERVED,
            )
        )

        incarceration_period_unterminated = StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_period_id=666,
            external_id="ip6",
            state_code="US_XX",
            admission_date=date(2008, 7, 15),
            admission_reason=AdmissionReason.NEW_ADMISSION,
        )

        incarceration_period_multiple_months_supervision_custodial_authority = (
            StateIncarcerationPeriod.new_with_defaults(
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                incarceration_period_id=444,
                external_id="ip4",
                state_code="US_XX",
                admission_date=date(2008, 7, 15),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2008, 9, 13),
                release_reason=ReleaseReason.SENTENCE_SERVED,
                custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            )
        )

        self.single_period_multiple_months_list = [incarceration_period_mulitple_months]
        self.single_period_partial_month_list = [incarceration_period_partial_month]
        self.single_period_unterminated_list = [incarceration_period_unterminated]

        self.multiple_periods_middle_of_month_consecutive_list = [
            incarceration_period_partial_month,
            incarceration_period_partial_month_2,
        ]
        self.multiple_periods_multiple_months_consecutive_list = [
            incarceration_period_mulitple_months,
            incarceration_period_mulitple_months_2,
        ]

        self.multiple_periods_middle_of_month_with_gap_in_month_list = [
            incarceration_period_partial_month,
            incarceration_period_partial_month_3,
        ]

        self.multiple_periods_large_gap_list = [
            incarceration_period_partial_month,
            incarceration_period_partial_month_2,
            incarceration_period_mulitple_months,
        ]

        self.multiple_periods_multiple_months_with_supervision_authority_consecutive_list = [
            incarceration_period_mulitple_months,
            incarceration_period_mulitple_months_2,
            incarceration_period_multiple_months_supervision_custodial_authority,
        ]

    def run_is_excluded_from_supervision_population_for_range_check(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
        range_start_num_days_from_periods_start: int,
        range_end_num_days_from_periods_end: int,
        is_excluded_from_supervision_population: bool,
    ):
        """Runs a test for the is_excluded_from_supervision_population function with the given parameters."""
        period_range_start = incarceration_periods[0].admission_date
        if not period_range_start:
            raise ValueError("Expected admission date")

        period_range_end = date_or_tomorrow(incarceration_periods[-1].release_date)

        lower_bound_inclusive = period_range_start + timedelta(
            days=range_start_num_days_from_periods_start
        )
        upper_bound_exclusive = period_range_end + timedelta(
            days=range_end_num_days_from_periods_end
        )

        index = _get_incarceration_period_index(incarceration_periods)

        time_range = DateRange(
            lower_bound_inclusive_date=lower_bound_inclusive,
            upper_bound_exclusive_date=upper_bound_exclusive,
        )
        if is_excluded_from_supervision_population:
            self.assertTrue(
                index.is_excluded_from_supervision_population_for_range(time_range)
            )
        else:
            self.assertFalse(
                index.is_excluded_from_supervision_population_for_range(time_range)
            )

    def test_no_periods(self):

        index = _get_incarceration_period_index([])
        self.assertFalse(
            index.is_excluded_from_supervision_population_for_range(
                DateRange(
                    lower_bound_inclusive_date=date(2019, 1, 2),
                    upper_bound_exclusive_date=date(2020, 2, 1),
                )
            )
        )

        self.assertFalse(
            index.is_excluded_from_supervision_population_for_range(
                DateRange(
                    lower_bound_inclusive_date=date(2019, 1, 1),
                    upper_bound_exclusive_date=date(2019, 2, 1),
                )
            )
        )

        self.assertFalse(
            index.is_excluded_from_supervision_population_for_range(
                DateRange(
                    lower_bound_inclusive_date=date(2019, 1, 1),
                    upper_bound_exclusive_date=date(2019, 1, 2),
                )
            )
        )

        self.assertFalse(
            index.is_excluded_from_supervision_population_for_range(
                DateRange(
                    lower_bound_inclusive_date=date(2019, 1, 1),
                    upper_bound_exclusive_date=date(2019, 1, 1),
                )
            )
        )

    @freeze_time("2008-07-18")
    def test_one_period_ranges_do_not_overlap(self):
        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_multiple_months_list,
            range_start_num_days_from_periods_start=360,
            range_end_num_days_from_periods_end=365,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_multiple_months_list,
            range_start_num_days_from_periods_start=-360,
            range_end_num_days_from_periods_end=-355,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_partial_month_list,
            range_start_num_days_from_periods_start=10,
            range_end_num_days_from_periods_end=13,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_partial_month_list,
            range_start_num_days_from_periods_start=-10,
            range_end_num_days_from_periods_end=-7,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_partial_month_list,
            range_start_num_days_from_periods_start=3,
            range_end_num_days_from_periods_end=5,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_unterminated_list,
            range_start_num_days_from_periods_start=-10,
            range_end_num_days_from_periods_end=-7,
            is_excluded_from_supervision_population=False,
        )

    @freeze_time("2008-07-18")
    def test_one_period_ranges_overlap_partially(self):
        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_multiple_months_list,
            range_start_num_days_from_periods_start=-2,
            range_end_num_days_from_periods_end=-5,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_multiple_months_list,
            range_start_num_days_from_periods_start=2,
            range_end_num_days_from_periods_end=5,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_multiple_months_list,
            range_start_num_days_from_periods_start=-2,
            range_end_num_days_from_periods_end=5,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_partial_month_list,
            range_start_num_days_from_periods_start=-2,
            range_end_num_days_from_periods_end=-5,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_partial_month_list,
            range_start_num_days_from_periods_start=2,
            range_end_num_days_from_periods_end=5,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_unterminated_list,
            range_start_num_days_from_periods_start=-10,
            range_end_num_days_from_periods_end=-1,
            is_excluded_from_supervision_population=False,
        )

    def test_one_period_ranges_overlap_partially_off_by_one_day(self):
        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_multiple_months_list,
            range_start_num_days_from_periods_start=-1,
            range_end_num_days_from_periods_end=-1,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_multiple_months_list,
            range_start_num_days_from_periods_start=1,
            range_end_num_days_from_periods_end=1,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_multiple_months_list,
            range_start_num_days_from_periods_start=-1,
            range_end_num_days_from_periods_end=1,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_partial_month_list,
            range_start_num_days_from_periods_start=-1,
            range_end_num_days_from_periods_end=-1,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_partial_month_list,
            range_start_num_days_from_periods_start=1,
            range_end_num_days_from_periods_end=1,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_partial_month_list,
            range_start_num_days_from_periods_start=-1,
            range_end_num_days_from_periods_end=1,
            is_excluded_from_supervision_population=False,
        )

    def test_one_period_range_overlaps_with_extra(self):

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_multiple_months_list,
            range_start_num_days_from_periods_start=5,
            range_end_num_days_from_periods_end=-100,
            is_excluded_from_supervision_population=True,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_multiple_months_list,
            range_start_num_days_from_periods_start=1,
            range_end_num_days_from_periods_end=-1,
            is_excluded_from_supervision_population=True,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_partial_month_list,
            range_start_num_days_from_periods_start=1,
            range_end_num_days_from_periods_end=-1,
            is_excluded_from_supervision_population=True,
        )

    def test_ranges_overlap_exactly(self):
        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_multiple_months_list,
            range_start_num_days_from_periods_start=0,
            range_end_num_days_from_periods_end=0,
            is_excluded_from_supervision_population=True,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.single_period_partial_month_list,
            range_start_num_days_from_periods_start=0,
            range_end_num_days_from_periods_end=0,
            is_excluded_from_supervision_population=True,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_middle_of_month_consecutive_list,
            range_start_num_days_from_periods_start=0,
            range_end_num_days_from_periods_end=0,
            is_excluded_from_supervision_population=True,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_multiple_months_consecutive_list,
            range_start_num_days_from_periods_start=0,
            range_end_num_days_from_periods_end=0,
            is_excluded_from_supervision_population=True,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_middle_of_month_with_gap_in_month_list,
            range_start_num_days_from_periods_start=0,
            range_end_num_days_from_periods_end=0,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_large_gap_list,
            range_start_num_days_from_periods_start=0,
            range_end_num_days_from_periods_end=0,
            is_excluded_from_supervision_population=False,
        )

        # Not excluded on the period under supervision authority
        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_multiple_months_with_supervision_authority_consecutive_list,
            range_start_num_days_from_periods_start=0,
            range_end_num_days_from_periods_end=0,
            is_excluded_from_supervision_population=False,
        )

        # Range doesn't include time of IP that's under supervision authority
        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_multiple_months_with_supervision_authority_consecutive_list,
            range_start_num_days_from_periods_start=0,
            range_end_num_days_from_periods_end=-60,
            is_excluded_from_supervision_population=True,
        )

    def test_two_consecutive_periods_ranges_do_not_overlap(self):
        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_multiple_months_consecutive_list,
            range_start_num_days_from_periods_start=360,
            range_end_num_days_from_periods_end=365,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_multiple_months_consecutive_list,
            range_start_num_days_from_periods_start=-360,
            range_end_num_days_from_periods_end=-355,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_middle_of_month_consecutive_list,
            range_start_num_days_from_periods_start=10,
            range_end_num_days_from_periods_end=13,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_middle_of_month_consecutive_list,
            range_start_num_days_from_periods_start=-10,
            range_end_num_days_from_periods_end=-7,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_middle_of_month_consecutive_list,
            range_start_num_days_from_periods_start=3,
            range_end_num_days_from_periods_end=5,
            is_excluded_from_supervision_population=False,
        )

    def test_two_consecutive_periods_ranges_overlap_partially(self):
        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_multiple_months_consecutive_list,
            range_start_num_days_from_periods_start=-2,
            range_end_num_days_from_periods_end=-5,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_multiple_months_consecutive_list,
            range_start_num_days_from_periods_start=2,
            range_end_num_days_from_periods_end=5,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_multiple_months_consecutive_list,
            range_start_num_days_from_periods_start=-2,
            range_end_num_days_from_periods_end=5,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_middle_of_month_consecutive_list,
            range_start_num_days_from_periods_start=-2,
            range_end_num_days_from_periods_end=-5,
            is_excluded_from_supervision_population=False,
        )

        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_middle_of_month_consecutive_list,
            range_start_num_days_from_periods_start=2,
            range_end_num_days_from_periods_end=5,
            is_excluded_from_supervision_population=False,
        )

        # Range only includes time of IP that's under supervision authority
        self.run_is_excluded_from_supervision_population_for_range_check(
            self.multiple_periods_multiple_months_with_supervision_authority_consecutive_list,
            range_start_num_days_from_periods_start=200,
            range_end_num_days_from_periods_end=0,
            is_excluded_from_supervision_population=False,
        )


class TestIncarcerationPeriodsNotUnderSupervisionAuthority(unittest.TestCase):
    """Tests the incarceration_periods_not_under_supervision_authority function."""

    def test_incarceration_periods_not_under_supervision_authority(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=444,
            external_id="ip4",
            state_code="US_XX",
            admission_date=date(2007, 12, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 2, 2),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # This period has a supervision custodial authority
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=555,
            external_id="ip5",
            state_code="US_XX",
            admission_date=date(2008, 2, 4),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 4, 5),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        index = _get_incarceration_period_index(
            [incarceration_period, incarceration_period_2]
        )

        self.assertEqual(
            [incarceration_period],
            index.incarceration_periods_not_under_supervision_authority,
        )

    def test_incarceration_periods_not_under_supervision_authority_all_authorities(
        self,
    ):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=444,
            external_id="ip4",
            state_code="US_XX",
            admission_date=date(2007, 12, 1),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 2, 2),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        all_custodial_authority_values = [None] + list(StateCustodialAuthority)

        for custodial_authority in all_custodial_authority_values:
            incarceration_period.custodial_authority = custodial_authority

            index = _get_incarceration_period_index([incarceration_period])

            expected_periods = (
                [incarceration_period]
                if incarceration_period.custodial_authority
                != StateCustodialAuthority.SUPERVISION_AUTHORITY
                else []
            )

            self.assertEqual(
                expected_periods,
                index.incarceration_periods_not_under_supervision_authority,
            )


class TestOriginalAdmissionReasonsByPeriodID(unittest.TestCase):
    """Tests the original_admission_reasons_by_period_id function."""

    def test_original_admission_reasons_by_period_id(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=222,
            admission_date=date(2000, 10, 3),
            release_date=date(2000, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        index = _get_incarceration_period_index(incarceration_periods)

        original_admission_reasons_by_period_id = (
            index.original_admission_reasons_by_period_id
        )

        reason_tuple = (
            incarceration_period_1.admission_reason,
            incarceration_period_1.admission_reason_raw_text,
        )

        expected_output = {
            incarceration_period_1.incarceration_period_id: reason_tuple,
            incarceration_period_2.incarceration_period_id: reason_tuple,
        }

        self.assertEqual(expected_output, original_admission_reasons_by_period_id)

    def test_original_admission_reasons_by_period_id_multiple_official_admissions(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=222,
            admission_date=date(2000, 10, 3),
            release_date=date(2000, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=333,
            admission_date=date(2020, 5, 1),
            release_date=date(2020, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_4 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=444,
            admission_date=date(2020, 10, 3),
            release_date=date(2020, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_periods = [
            incarceration_period_1,
            incarceration_period_2,
            incarceration_period_3,
            incarceration_period_4,
        ]

        index = _get_incarceration_period_index(incarceration_periods)

        original_admission_reasons_by_period_id = (
            index.original_admission_reasons_by_period_id
        )

        first_admission_tuple = (
            incarceration_period_1.admission_reason,
            incarceration_period_1.admission_reason_raw_text,
        )
        second_admission_tuple = (
            incarceration_period_3.admission_reason,
            incarceration_period_3.admission_reason_raw_text,
        )

        expected_output = {
            incarceration_period_1.incarceration_period_id: first_admission_tuple,
            incarceration_period_2.incarceration_period_id: first_admission_tuple,
            incarceration_period_3.incarceration_period_id: second_admission_tuple,
            incarceration_period_4.incarceration_period_id: second_admission_tuple,
        }

        self.assertEqual(expected_output, original_admission_reasons_by_period_id)

    def test_original_admission_reasons_by_period_id_multiple_transfer_periods(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=222,
            admission_date=date(2000, 10, 3),
            release_date=date(2000, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=333,
            admission_date=date(2000, 10, 12),
            release_date=date(2001, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_4 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=444,
            admission_date=date(2001, 1, 4),
            release_date=date(2001, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_periods = [
            incarceration_period_1,
            incarceration_period_2,
            incarceration_period_3,
            incarceration_period_4,
        ]

        index = _get_incarceration_period_index(incarceration_periods)

        original_admission_reasons_by_period_id = (
            index.original_admission_reasons_by_period_id
        )

        admission_tuple = (
            incarceration_period_1.admission_reason,
            incarceration_period_1.admission_reason_raw_text,
        )

        expected_output = {
            incarceration_period_1.incarceration_period_id: admission_tuple,
            incarceration_period_2.incarceration_period_id: admission_tuple,
            incarceration_period_3.incarceration_period_id: admission_tuple,
            incarceration_period_4.incarceration_period_id: admission_tuple,
        }

        self.assertEqual(expected_output, original_admission_reasons_by_period_id)

    def test_original_admission_reasons_by_period_id_no_official_admission(self):
        # The first incarceration period always counts as the official start of incarceration
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=222,
            admission_date=date(2000, 10, 3),
            release_date=date(2000, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        index = _get_incarceration_period_index(incarceration_periods)

        original_admission_reasons_by_period_id = (
            index.original_admission_reasons_by_period_id
        )

        admission_tuple = (
            incarceration_period_1.admission_reason,
            incarceration_period_1.admission_reason_raw_text,
        )

        expected_output = {
            incarceration_period_1.incarceration_period_id: admission_tuple,
            incarceration_period_2.incarceration_period_id: admission_tuple,
        }

        self.assertEqual(expected_output, original_admission_reasons_by_period_id)

    def test_original_admission_reasons_by_period_id_not_official_admission_after_official_release(
        self,
    ):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            # Because this is an official release, the next period should be mapped to its own admission reason
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=222,
            admission_date=date(2015, 10, 3),
            release_date=date(2015, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        index = _get_incarceration_period_index(incarceration_periods)

        original_admission_reasons_by_period_id = (
            index.original_admission_reasons_by_period_id
        )

        expected_output = {
            incarceration_period_1.incarceration_period_id: (
                incarceration_period_1.admission_reason,
                incarceration_period_1.admission_reason_raw_text,
            ),
            incarceration_period_2.incarceration_period_id: (
                incarceration_period_2.admission_reason,
                incarceration_period_2.admission_reason_raw_text,
            ),
        }

        self.assertEqual(expected_output, original_admission_reasons_by_period_id)

    def test_original_admission_reasons_by_period_id_not_official_admission_after_not_official_release(
        self,
    ):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=222,
            admission_date=date(2015, 10, 3),
            release_date=date(2015, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        index = _get_incarceration_period_index(incarceration_periods)

        original_admission_reasons_by_period_id = (
            index.original_admission_reasons_by_period_id
        )

        expected_output = {
            incarceration_period_1.incarceration_period_id: (
                incarceration_period_1.admission_reason,
                incarceration_period_1.admission_reason_raw_text,
            ),
            incarceration_period_2.incarceration_period_id: (
                incarceration_period_1.admission_reason,
                incarceration_period_1.admission_reason_raw_text,
            ),
        }

        self.assertEqual(expected_output, original_admission_reasons_by_period_id)


class TestPrecedingIncarcerationPeriod(unittest.TestCase):
    """Tests the preceding_incarceration_period_in_index function."""

    def test_preceding_incarceration_period_in_index_first_period(self) -> None:
        """Tests that this returns None when the given period is the first in the
        index."""
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=222,
            admission_date=date(2015, 10, 3),
            release_date=date(2015, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]
        index = _get_incarceration_period_index(incarceration_periods)

        preceding_period = index.preceding_incarceration_period_in_index(
            incarceration_period_1
        )
        self.assertIsNone(preceding_period)

    def test_preceding_incarceration_period_in_index_second_period(self) -> None:
        """Tests that this returns the first period when the given period is the
        second in the index."""
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=222,
            admission_date=date(2015, 10, 3),
            release_date=date(2015, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]
        index = _get_incarceration_period_index(incarceration_periods)

        preceding_period = index.preceding_incarceration_period_in_index(
            incarceration_period_2
        )
        self.assertEqual(incarceration_period_1, preceding_period)

    def test_preceding_incarceration_period_in_index_later_period(self) -> None:
        """Tests that this returns the period directly preceding the given period
        when it is not the first period."""
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=222,
            admission_date=date(2015, 10, 3),
            release_date=date(2015, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=333,
            admission_date=date(2019, 4, 8),
            release_date=date(2020, 10, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_periods = [
            incarceration_period_1,
            incarceration_period_2,
            incarceration_period_3,
        ]
        index = _get_incarceration_period_index(incarceration_periods)

        preceding_period = index.preceding_incarceration_period_in_index(
            incarceration_period_3
        )
        self.assertEqual(incarceration_period_2, preceding_period)

    def test_preceding_incarceration_period_in_index_not_in_index(self) -> None:
        """Tests that this raises a KeyError when the given period is not in the
        index."""
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=111,
            admission_date=date(2000, 1, 1),
            release_date=date(2000, 10, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=222,
            admission_date=date(2015, 10, 3),
            release_date=date(2015, 10, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=333,
            admission_date=date(2019, 4, 8),
            release_date=date(2020, 10, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_periods = [
            incarceration_period_1,
            incarceration_period_2,
        ]
        index = _get_incarceration_period_index(incarceration_periods)

        with self.assertRaises(KeyError):
            index.preceding_incarceration_period_in_index(incarceration_period_3)
