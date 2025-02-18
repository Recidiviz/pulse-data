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
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import DimensionLookMLViewField


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

    def test_validate_referenced_fields_exist_in_views_valid(self) -> None:
        filter_instance = LookMLDashboardFilter(
            name="test filter",
            field="view_name.field_name",
        )
        views = [
            LookMLView(
                view_name="view_name",
                fields=[
                    DimensionLookMLViewField(field_name="field_name", parameters=[]),
                    DimensionLookMLViewField(field_name="another_field", parameters=[]),
                ],
            ),
        ]
        try:
            filter_instance.validate_referenced_fields_exist_in_views(views)
        except ValueError:
            self.fail(
                "validate_referenced_fields_exist_in_views raised ValueError unexpectedly!"
            )

    def test_validate_referenced_fields_exist_in_views_invalid_field(self) -> None:
        filter_instance = LookMLDashboardFilter(
            name="test filter",
            field="view_name.invalid_field",
        )
        views = [
            LookMLView(
                view_name="view_name",
                fields=[
                    DimensionLookMLViewField(field_name="field_name", parameters=[]),
                    DimensionLookMLViewField(field_name="another_field", parameters=[]),
                ],
            ),
        ]
        with self.assertRaises(ValueError) as context:
            filter_instance.validate_referenced_fields_exist_in_views(views)
        self.assertIn(
            "Filter field [view_name.invalid_field] is not defined in view [view_name]",
            str(context.exception),
        )
