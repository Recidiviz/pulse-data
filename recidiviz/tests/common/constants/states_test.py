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

import importlib
import unittest
from unittest import mock

import us

from recidiviz.common.constants import states


class TestStates(unittest.TestCase):
    """Tests the functionality of the StateCode enum."""

    def setUp(self) -> None:
        self.in_test_patcher = mock.patch("recidiviz.utils.environment.in_test")
        self.mock_in_test = self.in_test_patcher.start()

    def tearDown(self) -> None:
        self.in_test_patcher.stop()

    def test_hasStates_not_in_test(self) -> None:
        self.mock_in_test.return_value = False
        importlib.reload(states)

        self.assertEqual(50, len(states.StateCode))
        self.assertEqual("US_AK", list(states.StateCode)[0].value)
        self.assertEqual("US_WY", list(states.StateCode)[-1].value)

    def test_hasStates_in_test(self) -> None:
        self.mock_in_test.return_value = True

        importlib.reload(states)

        # There are 51 states because we are in tests, so US_XX is a valid value
        self.assertEqual(51, len(states.StateCode))
        self.assertEqual("US_AK", list(states.StateCode)[0].value)
        self.assertEqual("US_WY", list(states.StateCode)[-2].value)
        self.assertEqual("US_XX", list(states.StateCode)[-1].value)

    def test_getState(self) -> None:
        importlib.reload(states)
        self.assertEqual(us.states.AK, states.StateCode.US_AK.get_state())

    def test_is_state_code_not_in_test(self) -> None:
        self.mock_in_test.return_value = False

        importlib.reload(states)

        valid_states = ["us_wa", "US_MD", "us_ma"]
        # US_XX is not a valid state_code outside of tests
        invalid_states = ["us_gu", "us_dc", "US_PR", "UX_CA", "us_xx", "US_XX"]

        for state_code in valid_states:
            self.assertTrue(states.StateCode.is_state_code(state_code))

        for state_code in invalid_states:
            self.assertFalse(states.StateCode.is_state_code(state_code))

    def test_is_state_code_in_test(self) -> None:
        self.mock_in_test.return_value = True

        importlib.reload(states)

        # US_XX is a valid state_code when we are in tests
        valid_states = ["us_xx", "US_XX"]

        for state_code in valid_states:
            self.assertTrue(states.StateCode.is_state_code(state_code))
