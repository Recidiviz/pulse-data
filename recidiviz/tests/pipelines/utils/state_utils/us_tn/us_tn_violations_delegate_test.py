#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
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
""""Tests the us_tn_violation_delegate.py file."""
import unittest
from datetime import date

from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import DateRange
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_violations_delegate import (
    UsTnViolationDelegate,
)

_STATE_CODE = StateCode.US_TN.value


class TestUsTnViolationsDelegate(unittest.TestCase):
    """Tests the us_tn_violations_delegate."""

    def setUp(self) -> None:
        self.delegate = UsTnViolationDelegate()

    # ~~ Add new tests here ~~


class TestViolationHistoryWindowPreCommitment(unittest.TestCase):
    """Tests the US_TN specific implementation of violation_history_window_relevant_to_critical_date
    function on the UsNDViolationDelegate."""

    def test_us_tn_violation_history_window_relevant_to_critical_date(
        self,
    ) -> None:
        violation_window = (
            UsTnViolationDelegate().violation_history_window_relevant_to_critical_date(
                critical_date=date(2000, 1, 1),
                sorted_and_filtered_violation_responses=[],
                default_violation_history_window_months=0,
            )
        )

        expected_violation_window = DateRange(
            # 12 months before
            lower_bound_inclusive_date=date(1999, 1, 1),
            # 10 days, including admission_date
            upper_bound_exclusive_date=date(2000, 1, 11),
        )

        self.assertEqual(expected_violation_window, violation_window)
