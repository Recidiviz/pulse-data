# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for states_utils.py"""
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.states_utils import find_state_codes_in_str


class TestFindStateCodesInStr(unittest.TestCase):
    """Tests for find_state_codes_in_str()."""

    def test_no_state_code(self) -> None:
        self.assertEqual(set(), find_state_codes_in_str("my_dataset"))
        self.assertEqual(set(), find_state_codes_in_str("prefix_us_states"))
        self.assertEqual(set(), find_state_codes_in_str(""))

    def test_state_code_at_start(self) -> None:
        self.assertEqual({StateCode.US_XX}, find_state_codes_in_str("us_xx_raw_data"))

    def test_state_code_in_middle(self) -> None:
        self.assertEqual(
            {StateCode.US_XX}, find_state_codes_in_str("prefix_us_xx_raw_data")
        )

    def test_state_code_at_end(self) -> None:
        self.assertEqual({StateCode.US_XX}, find_state_codes_in_str("my_dataset_us_xx"))

    def test_multiple_state_codes(self) -> None:
        self.assertEqual(
            {StateCode.US_XX, StateCode.US_YY},
            find_state_codes_in_str("us_xx_something_us_yy"),
        )
        self.assertEqual(
            {StateCode.US_XX, StateCode.US_YY},
            find_state_codes_in_str("something_us_xx_something_us_yy_something"),
        )

    def test_case_insensitive(self) -> None:
        self.assertEqual({StateCode.US_XX}, find_state_codes_in_str("US_XX_raw_data"))

    def test_invalid_state_code_ignored(self) -> None:
        self.assertEqual(set(), find_state_codes_in_str("us_zz_raw_data"))

    def test_multi_letter_state_code(self) -> None:
        # Codes of 3 or more letters are handled the same as 2-letter codes.
        self.assertEqual({StateCode.US_NYC}, find_state_codes_in_str("us_nyc_raw_data"))
        self.assertEqual(
            {StateCode.US_NYC}, find_state_codes_in_str("prefix_us_nyc_raw_data")
        )
        self.assertEqual(
            {StateCode.US_NYC}, find_state_codes_in_str("my_dataset_us_nyc")
        )
        # US_DEMO exercises a 4-letter code.
        self.assertEqual(
            {StateCode.US_DEMO}, find_state_codes_in_str("us_demo_raw_data")
        )

    def test_multi_letter_code_not_resolved_to_shorter_prefix(self) -> None:
        # A tenant code must resolve whole, never to a shorter registered prefix:
        # "us_nyc" is US_NYC (not US_NY), "us_demo" is US_DEMO (not US_DE).
        self.assertEqual({StateCode.US_NYC}, find_state_codes_in_str("us_nyc"))
        self.assertNotIn(StateCode.US_NY, find_state_codes_in_str("us_nyc"))
        self.assertEqual({StateCode.US_DEMO}, find_state_codes_in_str("us_demo"))
        self.assertNotIn(StateCode.US_DE, find_state_codes_in_str("us_demo"))

    def test_three_and_two_letter_codes_together(self) -> None:
        self.assertEqual(
            {StateCode.US_XX, StateCode.US_NYC},
            find_state_codes_in_str("us_xx_something_us_nyc"),
        )

    def test_overlapping_parent_and_child_codes_together(self) -> None:
        # When a 2-letter parent (US_NY) and its overlapping longer tenant code
        # (US_NYC) both appear explicitly, both are returned.
        self.assertEqual(
            {StateCode.US_NY, StateCode.US_NYC},
            find_state_codes_in_str("us_ny_data_us_nyc"),
        )
