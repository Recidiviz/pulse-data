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
""""Tests the us_mi_violation_delegate.py file."""
import unittest
from datetime import date

from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import DateRange
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_violations_delegate import (
    UsMiViolationDelegate,
)

_STATE_CODE = StateCode.US_MI.value


class TestUsMiViolationsDelegate(unittest.TestCase):
    """Tests the us_mi_violations_delegate."""

    def setUp(self) -> None:
        self.delegate = UsMiViolationDelegate()

    # ~~ Add new tests here ~~


class TestViolationHistoryWindowPreCommitment(unittest.TestCase):
    """Tests the US_MI specific implementation of violation_history_window_relevant_to_critical_date
    function on the UsMiViolationDelegate."""

    def test_us_mi_violation_history_window_relevant_to_critical_date(
        self,
    ) -> None:
        violation_window = (
            UsMiViolationDelegate().violation_history_window_relevant_to_critical_date(
                critical_date=date(2000, 1, 1),
                sorted_and_filtered_violation_responses=[],
                default_violation_history_window_months=0,
            )
        )

        expected_violation_window = DateRange(
            # 24 months before
            lower_bound_inclusive_date=date(1998, 1, 1),
            # Admission Date
            upper_bound_exclusive_date=date(2000, 1, 1),
        )

        self.assertEqual(expected_violation_window, violation_window)
