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
"""Tests for incarceration_period_utils.py."""

import unittest
from datetime import date

from recidiviz.calculator.pipeline.utils.incarceration_period_utils import (
    ip_is_nested_in_previous_period,
    period_edges_are_valid_transfer,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class TestIpIsNestedInPreviousPeriod(unittest.TestCase):
    """Tests the ip_is_nested_in_previous_period function."""

    def test_ip_is_nested_in_previous_period(self):
        previous_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2002, 3, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 5, 22),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        self.assertTrue(ip_is_nested_in_previous_period(ip, previous_ip))

    def test_ip_is_nested_in_previous_period_not_nested(self):
        previous_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2002, 10, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 12, 22),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        self.assertFalse(ip_is_nested_in_previous_period(ip, previous_ip))

    def test_ip_is_nested_in_previous_period_share_release(self):
        previous_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2002, 3, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        self.assertTrue(ip_is_nested_in_previous_period(ip, previous_ip))

    def test_ip_is_nested_in_previous_period_zero_day_period(self):
        previous_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2002, 3, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 3, 13),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        self.assertTrue(ip_is_nested_in_previous_period(ip, previous_ip))

    def test_ip_is_nested_in_previous_period_two_zero_day_periods(self):
        previous_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 3, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 3, 13),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2002, 3, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 3, 13),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        self.assertFalse(ip_is_nested_in_previous_period(ip, previous_ip))

    def test_ip_is_nested_in_previous_period_zero_day_period_on_release(self):
        previous_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 1, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 3, 13),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2002, 3, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 3, 13),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        self.assertFalse(ip_is_nested_in_previous_period(ip, previous_ip))

    def test_ip_is_nested_in_previous_period_bad_sort(self):
        # This period should not have been sorted before ip
        previous_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2002, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 5, 22),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        with self.assertRaises(ValueError):
            ip_is_nested_in_previous_period(ip, previous_ip)


class TestPeriodEdgesAreValidTransfer(unittest.TestCase):
    """Tests the period_edges_are_valid_transfer function."""

    def test_period_edges_are_valid_transfer(self):
        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 9, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 9, 19),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        self.assertTrue(period_edges_are_valid_transfer(ip_1, ip_2))

    def test_period_edges_are_valid_transfer_too_far(self):
        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 9, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 9, 19),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        self.assertFalse(period_edges_are_valid_transfer(ip_1, ip_2))

    def test_period_edges_are_valid_transfer_not_transfers(self):
        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 9, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2002, 9, 19),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        self.assertFalse(period_edges_are_valid_transfer(ip_1, ip_2))
