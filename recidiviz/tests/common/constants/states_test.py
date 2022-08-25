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
"""Tests for state constants"""

import unittest
from unittest import mock

import us

from recidiviz.common.constants import states
from recidiviz.common.constants.states import MAX_FIPS_CODE, StateCode


class TestStates(unittest.TestCase):
    """Tests the functionality of the StateCode enum."""

    def setUp(self) -> None:
        self.in_test_patcher = mock.patch("recidiviz.utils.environment.in_test")
        self.mock_in_test = self.in_test_patcher.start()

    def tearDown(self) -> None:
        self.in_test_patcher.stop()

    def test_hasStates_not_in_test(self) -> None:
        self.mock_in_test.return_value = False

        # There are 52 states because we include US_DC, as well as a playground region
        # US_OZ.
        self.assertEqual(52, len(states.StateCode))
        self.assertEqual("US_AK", list(states.StateCode)[0].value)
        self.assertEqual("US_WY", list(states.StateCode)[-2].value)
        self.assertEqual("US_OZ", list(states.StateCode)[-1].value)

    def test_hasStates_in_test(self) -> None:
        self.mock_in_test.return_value = True

        # There are 55 states because we are in tests, so we add US_WW, US_XX, and US_YY
        # as valid values.
        self.assertEqual(55, len(states.StateCode))
        self.assertEqual("US_AK", list(states.StateCode)[0].value)
        self.assertEqual("US_WY", list(states.StateCode)[-5].value)
        self.assertEqual("US_OZ", list(states.StateCode)[-4].value)
        self.assertEqual("US_WW", list(states.StateCode)[-3].value)
        self.assertEqual("US_XX", list(states.StateCode)[-2].value)
        self.assertEqual("US_YY", list(states.StateCode)[-1].value)

    def test_getState(self) -> None:
        self.assertEqual(us.states.AK, states.StateCode.US_AK.get_state())

    def test_is_state_code_not_in_test(self) -> None:
        self.mock_in_test.return_value = False

        valid_states = ["us_wa", "US_MD", "us_ma"]
        # US_XX is not a valid state_code outside of tests
        invalid_states = ["us_gu", "US_PR", "UX_CA", "us_xx", "US_XX"]

        for state_code in valid_states:
            self.assertTrue(states.StateCode.is_state_code(state_code))

        for state_code in invalid_states:
            self.assertFalse(states.StateCode.is_state_code(state_code))

    def test_is_state_code_in_test(self) -> None:
        self.mock_in_test.return_value = True

        # US_XX is a valid state_code when we are in tests
        valid_states = ["us_xx", "US_XX"]

        for state_code in valid_states:
            self.assertTrue(states.StateCode.is_state_code(state_code))

    def test_invalid_state_code(self) -> None:
        invalid_state_code = "US_XX_YYY"
        with self.assertRaises(ValueError):
            _ = StateCode(invalid_state_code)

        self.assertEqual(None, StateCode.get(invalid_state_code))

    def test_max_fips_code(self) -> None:
        self.mock_in_test.return_value = False

        max_fips_value = -1

        for state_code in StateCode:
            if states.StateCode.is_state_code(state_code.value):
                fips_value = int(state_code.get_state().fips)
                max_fips_value = max(max_fips_value, fips_value)

        self.assertEqual(
            MAX_FIPS_CODE,
            max_fips_value,
            f"Found maximum fips value of {max_fips_value}. "
            f"Must update MAX_FIPS_CODE to match.",
        )
