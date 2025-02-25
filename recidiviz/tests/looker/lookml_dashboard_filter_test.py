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
"""Tests functionality of LookMLDashboard filter"""
import unittest

from recidiviz.looker.lookml_dashboard_filter import (
    LookMLDashboardFilter,
    LookMLFilterType,
    LookMLFilterUIConfig,
    LookMLFilterUIDisplay,
    LookMLFilterUIType,
)


class LookMLDashboardFilterTest(unittest.TestCase):
    """
    Tests LookMLDashboardFilter
    """

    def test_dashboard_filter_no_attributes(self) -> None:
        # Test a filter without attributes
        filter_output = LookMLDashboardFilter(name="test filter").build()
        expected_result = "- name: test filter"
        self.assertEqual(filter_output, expected_result)

    def test_dashboard_filter_many_attributes(self) -> None:
        # Test a filter with many attributes filled in
        filter_output = LookMLDashboardFilter(
            name="test filter",
            title="test title",
            type=LookMLFilterType.FIELD_FILTER,
            default_value="default",
            allow_multiple_values=False,
            required=True,
            ui_config=LookMLFilterUIConfig(
                LookMLFilterUIType.DROPDOWN_MENU,
                LookMLFilterUIDisplay.INLINE,
            ),
            model="model",
            explore="explore",
            field="view_name.field_name",
        ).build()
        expected_result = """- name: test filter
    title: test title
    type: field_filter
    default_value: default
    allow_multiple_values: false
    required: true
    ui_config: 
      type: dropdown_menu
      display: inline
    model: model
    explore: explore
    field: view_name.field_name"""
        self.assertEqual(filter_output, expected_result)
