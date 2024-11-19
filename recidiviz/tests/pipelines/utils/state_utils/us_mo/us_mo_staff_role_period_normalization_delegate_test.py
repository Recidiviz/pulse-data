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
from recidiviz.common.date import current_date_us_eastern
from recidiviz.persistence.entity.entity_utils import deep_entity_update
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

        todays_date_eastern = current_date_us_eastern()

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
            end_date=todays_date_eastern
            + timedelta(
                days=5
            ),  # This end date is in the future and should be invalidated.
            external_id="2",
        )

        rp_both_dates_in_future = StateStaffRolePeriod.new_with_defaults(
            state_code=StateCode.US_MO.value,
            role_type=StateStaffRoleType.INTERNAL_UNKNOWN,
            role_type_raw_text="OTHER",
            start_date=todays_date_eastern + timedelta(days=5),
            end_date=todays_date_eastern
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
            rp_valid_start_date_only,
            rp_both_valid_dates,
        ]

        expected_rp_start_date_too_far_in_past = deep_entity_update(
            copy.deepcopy(rp_start_date_too_far_in_past), start_date=date(1900, 1, 1)
        )
        expected_rp_end_date_in_future = deep_entity_update(
            copy.deepcopy(rp_end_date_in_future), end_date=None
        )
        expected_rp_both_dates_in_future = deep_entity_update(
            copy.deepcopy(rp_both_dates_in_future),
            start_date=todays_date_eastern,
            end_date=None,
        )
        # The expected versions of these 2 periods post-normalization are just copies of
        # the original periods, as they shouldn't be touched in normalization.
        expected_rp_valid_start_date_only = copy.deepcopy(rp_valid_start_date_only)
        expected_rp_both_valid_dates = copy.deepcopy(rp_both_valid_dates)

        expected_normalized_rps = [
            expected_rp_start_date_too_far_in_past,
            expected_rp_end_date_in_future,
            expected_rp_both_dates_in_future,
            # Note that a normalized version of rp_dates_out_of_order is not included in
            # this list, since the period gets removed in normalization.
            expected_rp_valid_start_date_only,
            expected_rp_both_valid_dates,
        ]

        normalized_rps = self.delegate.normalize_role_periods(copy.deepcopy(rps))

        self.assertEqual(normalized_rps, expected_normalized_rps)
