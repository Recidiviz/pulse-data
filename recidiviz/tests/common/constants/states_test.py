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
    def test_hasStates(self):
        self.assertEqual(50, len(states.StateCode))
        self.assertEqual('US_AK', list(states.StateCode)[0].value)
        self.assertEqual('US_WY', list(states.StateCode)[-1].value)

    def test_getState(self):
        self.assertEqual(us.states.AK, states.StateCode.US_AK.get_state())
