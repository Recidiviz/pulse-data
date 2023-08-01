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

from recidiviz.looker.lookml_dashboard import LookMLDashboard
from recidiviz.looker.lookml_dashboard_filter import LookMLDashboardFilter


class LookMLDashboardTest(unittest.TestCase):
    """Tests correctness of LookML dashboard generation"""

    def test_no_filters_dashboard(self) -> None:
        # Not including filters throws an error.
        with self.assertRaisesRegex(ValueError, r"Length of 'filters' must be => 1: 0"):
            _ = LookMLDashboard(
                dashboard_name="test_dashboard",
                parameters=[],
                filters=[],
            )

    def test_empty_lookml_dashboard(self) -> None:
        # Empty dashboard, basic filter
        dashboard = LookMLDashboard(
            dashboard_name="test_dashboard",
            parameters=[],
            filters=[LookMLDashboardFilter(name="test filter")],
        ).build()
        expected = """- dashboard: test_dashboard
  

  filters:
  - name: test filter
"""
        self.assertEqual(dashboard, expected)

    def test_lookml_dashboard_display_parameters(self) -> None:
        # Empty dashboard with extension required, an extended
        # dashboard and load configuration
        dashboard = LookMLDashboard(
            dashboard_name="test_dashboard",
            load_configuration_wait=True,
            extension_required=True,
            extended_dashboard="test_extended_dashboard",
            parameters=[],
            filters=[LookMLDashboardFilter(name="test filter")],
        ).build()
        expected = """- dashboard: test_dashboard
  load_configuration: wait
  extends: test_extended_dashboard
  extension: required

  filters:
  - name: test filter"""
        self.assertEqual(dashboard.strip(), expected)
