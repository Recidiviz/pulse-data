# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests functionality of EventType functions"""

import unittest

from recidiviz.observations.event_type import EventType


class EventTypeTest(unittest.TestCase):
    """
    Tests EventType functions
    """

    def test_unit_of_observation_type(self) -> None:
        """Check that unit_of_observation_type is defined for every EventType enum"""
        for event_type in EventType:
            self.assertIsNotNone(event_type.unit_of_observation_type)
