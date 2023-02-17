#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests for the state_specific_violations_delegate."""
import unittest
from datetime import date

from recidiviz.calculator.pipeline.metrics.utils.violation_utils import (
    VIOLATION_HISTORY_WINDOW_MONTHS,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_violations_delegate import (
    UsXxViolationDelegate,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.common.date import DateRange


class TestDefaultViolationHistoryWindowPreCriticalDate(unittest.TestCase):
    """Tests the default behavior of the
    violation_history_window_pre_critical_date function on the
    StateSpecificViolationsDelegate."""

    def test_default_violation_history_window_pre_critical_date(
        self,
    ) -> None:
        state_code = "US_XX"

        supervision_violation_response_1 = (
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                state_code=state_code,
                supervision_violation_response_id=123,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2008, 12, 7),
            )
        )

        supervision_violation_response_2 = (
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=234,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                state_code=state_code,
                response_date=date(2009, 11, 13),
            )
        )

        supervision_violation_response_3 = (
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                state_code=state_code,
                supervision_violation_response_id=345,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2009, 12, 1),
            )
        )

        violation_window = (
            UsXxViolationDelegate().violation_history_window_pre_critical_date(
                critical_date=date(2009, 12, 14),
                sorted_and_filtered_violation_responses=[
                    supervision_violation_response_1,
                    supervision_violation_response_2,
                    supervision_violation_response_3,
                ],
                default_violation_history_window_months=VIOLATION_HISTORY_WINDOW_MONTHS,
            )
        )

        expected_violation_window = DateRange(
            lower_bound_inclusive_date=date(2008, 12, 1),
            upper_bound_exclusive_date=date(2009, 12, 2),
        )

        self.assertEqual(expected_violation_window, violation_window)

    def test_default_violation_history_window_pre_critical_date_filter_after(
        self,
    ) -> None:
        state_code = "US_XX"

        supervision_violation_response_1 = (
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                state_code=state_code,
                supervision_violation_response_id=123,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2008, 12, 7),
            )
        )

        supervision_violation_response_2 = (
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=234,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                state_code=state_code,
                response_date=date(2009, 11, 13),
            )
        )

        # This is after the critical_date
        supervision_violation_response_3 = (
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                state_code=state_code,
                supervision_violation_response_id=345,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2012, 12, 1),
            )
        )

        violation_window = (
            UsXxViolationDelegate().violation_history_window_pre_critical_date(
                critical_date=date(2009, 12, 14),
                sorted_and_filtered_violation_responses=[
                    supervision_violation_response_1,
                    supervision_violation_response_2,
                    supervision_violation_response_3,
                ],
                default_violation_history_window_months=VIOLATION_HISTORY_WINDOW_MONTHS,
            )
        )

        expected_violation_window = DateRange(
            lower_bound_inclusive_date=date(2008, 11, 13),
            upper_bound_exclusive_date=date(2009, 11, 14),
        )

        self.assertEqual(expected_violation_window, violation_window)

    def test_default_violation_history_window_pre_critical_date_no_responses(
        self,
    ) -> None:
        violation_window = (
            UsXxViolationDelegate().violation_history_window_pre_critical_date(
                critical_date=date(2009, 12, 14),
                sorted_and_filtered_violation_responses=[],
                default_violation_history_window_months=VIOLATION_HISTORY_WINDOW_MONTHS,
            )
        )

        expected_violation_window = DateRange(
            lower_bound_inclusive_date=date(2008, 12, 14),
            upper_bound_exclusive_date=date(2009, 12, 15),
        )

        self.assertEqual(expected_violation_window, violation_window)
