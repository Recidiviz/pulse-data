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
"""Tests the US_MO-specific UsMoStaffRolePeriodNormalizationManager."""
import copy
import unittest
from datetime import date, timedelta

from recidiviz.common.constants.state.state_staff_role_period import StateStaffRoleType
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateStaffRolePeriod
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_staff_role_period_normalization_delegate import (
    UsMoStaffRolePeriodNormalizationDelegate,
)


class TestUsMoStaffRolePeriodDelegate(unittest.TestCase):
    """Tests functions in TestUsMoStaffRolePeriodNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsMoStaffRolePeriodNormalizationDelegate()

    def test_staff_role_periods_date_validation(
        self,
    ) -> None:
        """Assert that improperly entered role period dates are invalidated."""
        rp_start_date_too_far_in_past = StateStaffRolePeriod.new_with_defaults(
            state_code=StateCode.US_MO.value,
            role_type=StateStaffRoleType.INTERNAL_UNKNOWN,
            role_type_raw_text="OTHER",
            start_date=date(
                201, 1, 1
            ),  # This start date is too far in the past and should be invalidated.
            end_date=date(2015, 1, 1),
            external_id="1",
        )

        rp_end_date_in_future = StateStaffRolePeriod.new_with_defaults(
            state_code=StateCode.US_MO.value,
            role_type=StateStaffRoleType.INTERNAL_UNKNOWN,
            role_type_raw_text="OTHER",
            start_date=date(2010, 1, 1),
            end_date=date.today()
            + timedelta(
                days=5
            ),  # This end date is in the future and should be invalidated.
            external_id="2",
        )

        rp_both_dates_in_future = StateStaffRolePeriod.new_with_defaults(
            state_code=StateCode.US_MO.value,
            role_type=StateStaffRoleType.INTERNAL_UNKNOWN,
            role_type_raw_text="OTHER",
            start_date=date.today() + timedelta(days=5),
            end_date=date.today()
            + timedelta(
                days=365
            ),  # Both of these dates are in the future and should be invalidated.
            external_id="3",
        )

        rp_dates_out_of_order = StateStaffRolePeriod.new_with_defaults(
            state_code=StateCode.US_MO.value,
            role_type=StateStaffRoleType.INTERNAL_UNKNOWN,
            role_type_raw_text="OTHER",
            start_date=date(2010, 1, 1),
            end_date=date(
                2009, 1, 1
            ),  # The start date is after the end date; both should be invalidated.
            external_id="4",
        )

        rp_dates_out_of_order_and_start_in_future = StateStaffRolePeriod.new_with_defaults(
            state_code=StateCode.US_MO.value,
            role_type=StateStaffRoleType.INTERNAL_UNKNOWN,
            role_type_raw_text="OTHER",
            start_date=date.today() + timedelta(days=5),
            end_date=date(2011, 1, 1),
            # The start date is in the future and the end date looks valid, but BOTH dates
            # should still be invalidated because the dates are out of order.
            external_id="5",
        )

        rp_valid_start_date_only = StateStaffRolePeriod.new_with_defaults(
            state_code=StateCode.US_MO.value,
            role_type=StateStaffRoleType.INTERNAL_UNKNOWN,
            role_type_raw_text="OTHER",
            start_date=date(2010, 1, 1),  # This is valid and should be left alone.
            external_id="6",
        )

        rp_both_valid_dates = StateStaffRolePeriod.new_with_defaults(
            state_code=StateCode.US_MO.value,
            role_type=StateStaffRoleType.INTERNAL_UNKNOWN,
            role_type_raw_text="OTHER",
            start_date=date(2010, 1, 1),
            end_date=date(
                2011, 1, 1
            ),  # Both of these dates are valid and should be left alone.
            external_id="7",
        )

        rps = [
            rp_start_date_too_far_in_past,
            rp_end_date_in_future,
            rp_both_dates_in_future,
            rp_dates_out_of_order,
            rp_dates_out_of_order_and_start_in_future,
            rp_valid_start_date_only,
            rp_both_valid_dates,
        ]

        normalized_rps = self.delegate.normalize_role_periods(copy.deepcopy(rps))

        normalized_rp_start_date_too_far_in_past = normalized_rps[0]
        normalized_rp_end_date_in_future = normalized_rps[1]
        normalized_rp_both_dates_in_future = normalized_rps[2]
        normalized_rp_dates_out_of_order = normalized_rps[3]
        normalized_rp_dates_out_of_order_and_start_in_future = normalized_rps[4]
        normalized_rp_valid_start_date_only = normalized_rps[5]
        normalized_rp_both_valid_dates = normalized_rps[6]

        self.assertIsNone(normalized_rp_start_date_too_far_in_past.start_date)
        self.assertEqual(
            normalized_rp_start_date_too_far_in_past.end_date,
            rp_start_date_too_far_in_past.end_date,
        )

        self.assertIsNone(normalized_rp_end_date_in_future.end_date)
        self.assertEqual(
            normalized_rp_end_date_in_future.start_date,
            rp_end_date_in_future.start_date,
        )

        self.assertIsNone(normalized_rp_both_dates_in_future.start_date)
        self.assertIsNone(normalized_rp_both_dates_in_future.end_date)

        self.assertIsNone(normalized_rp_dates_out_of_order.start_date)
        self.assertIsNone(normalized_rp_dates_out_of_order.end_date)

        self.assertIsNone(
            normalized_rp_dates_out_of_order_and_start_in_future.start_date
        )
        self.assertIsNone(normalized_rp_dates_out_of_order_and_start_in_future.end_date)

        self.assertEqual(normalized_rp_valid_start_date_only, rp_valid_start_date_only)

        self.assertEqual(normalized_rp_both_valid_dates, rp_both_valid_dates)
