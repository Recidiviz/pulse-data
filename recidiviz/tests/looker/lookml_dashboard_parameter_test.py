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
"""Tests functionality of LookMLDashboard functions"""
import unittest

from recidiviz.looker.lookml_dashboard_parameter import (
    LookMLDashboardLayoutType,
    LookMLDashboardParameter,
)


class LookMLDashboardParameterTest(unittest.TestCase):
    """
    Tests LookMLDashboardParameter
    """

    def test_display_parameters(self) -> None:
        # Test display parameter types
        title = LookMLDashboardParameter.title("test title").build()
        expected_title = "title: test title"
        self.assertEqual(title, expected_title)

        description = LookMLDashboardParameter.description("test description").build()
        expected_description = "description: test description"
        self.assertEqual(description, expected_description)

        layout = LookMLDashboardParameter.layout(
            LookMLDashboardLayoutType.NEWSPAPER
        ).build()
        expected_layout = "layout: newspaper"
        self.assertEqual(layout, expected_layout)
