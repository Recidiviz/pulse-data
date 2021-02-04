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

import us

from recidiviz.common.constants import states


class TestStates(unittest.TestCase):
    def test_hasStates(self) -> None:
        self.assertEqual(50, len(states.StateCode))
        self.assertEqual('US_AK', list(states.StateCode)[0].value)
        self.assertEqual('US_WY', list(states.StateCode)[-1].value)

    def test_getState(self) -> None:
        self.assertEqual(us.states.AK, states.StateCode.US_AK.get_state())

    def test_is_state_code(self) -> None:
        valid_states = ['us_wa', 'US_MD', 'us_ma']
        invalid_states = ['us_xx', 'US_XX', 'us_gu', 'us_dc', 'US_PR', 'UX_CA', ]

        for state_code in valid_states:
            self.assertTrue(states.StateCode.is_state_code(state_code))

        for state_code in invalid_states:
            self.assertFalse(states.StateCode.is_state_code(state_code))
