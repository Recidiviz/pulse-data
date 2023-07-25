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
"""Tests functionality of LookMLExploreParameter types"""

import unittest

from recidiviz.looker.lookml_explore_parameter import LookMLExploreParameter


class LookMLExploreParameterTest(unittest.TestCase):
    def test_display_parameters(self) -> None:
        # Test display parameter types
        description = LookMLExploreParameter.description("test description").build()
        expected_description = 'description: "test description"'
        self.assertEqual(description, expected_description)

        group_label = LookMLExploreParameter.group_label("test group label").build()
        expected_group_label = 'group_label: "test group label"'
        self.assertEqual(group_label, expected_group_label)
