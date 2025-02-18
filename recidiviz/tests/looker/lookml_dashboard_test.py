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
from recidiviz.looker.lookml_dashboard_element import LookMLDashboardElement
from recidiviz.looker.lookml_dashboard_filter import LookMLDashboardFilter
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import DimensionLookMLViewField


class LookMLDashboardTest(unittest.TestCase):
    """Tests correctness of LookML dashboard generation"""

    def test_no_filters_dashboard(self) -> None:
        # Not including filters throws an error.
        with self.assertRaisesRegex(ValueError, r"Length of 'filters' must be >= 1: 0"):
            _ = LookMLDashboard(
                dashboard_name="test_dashboard",
                parameters=[],
                filters=[],
                elements=[LookMLDashboardElement(name="test element")],
            )

    def test_no_elements_dashboard(self) -> None:
        # Not including elements throws an error.
        with self.assertRaisesRegex(
            ValueError, r"Length of 'elements' must be >= 1: 0"
        ):
            _ = LookMLDashboard(
                dashboard_name="test_dashboard",
                parameters=[],
                filters=[LookMLDashboardFilter(name="test element")],
                elements=[],
            )

    def test_empty_lookml_dashboard(self) -> None:
        # Empty dashboard, basic filter and element
        dashboard = LookMLDashboard(
            dashboard_name="test_dashboard",
            parameters=[],
            filters=[LookMLDashboardFilter(name="test filter")],
            elements=[LookMLDashboardElement(name="test element")],
        ).build()
        expected = """- dashboard: test_dashboard
  

  filters:
  - name: test filter

  elements:
  - name: test element
"""
        self.assertEqual(dashboard, expected)

    def test_lookml_dashboard_display_parameters(self) -> None:
        # Empty dashboard with extension required, an extended
        # dashboard and load configuration, and more complex lists of filters/elements
        dashboard = LookMLDashboard(
            dashboard_name="test_dashboard",
            load_configuration_wait=True,
            extension_required=True,
            extended_dashboard="test_extended_dashboard",
            parameters=[],
            filters=[
                LookMLDashboardFilter(name="test filter", field="view_name.field_name"),
                LookMLDashboardFilter(name="test filter 2"),
            ],
            elements=[
                LookMLDashboardElement(name="test element"),
                LookMLDashboardElement(name="test element 2", title="test title"),
            ],
        ).build()
        expected = """- dashboard: test_dashboard
  load_configuration: wait
  extends: test_extended_dashboard
  extension: required

  filters:
  - name: test filter
    field: view_name.field_name

  - name: test filter 2

  elements:
  - name: test element

  - name: test element 2
    title: test title"""
        self.assertEqual(dashboard.strip(), expected)

    def test_validate_referenced_fields_exist_in_views_valid(self) -> None:
        views = [
            LookMLView(
                view_name="view_name",
                fields=[
                    DimensionLookMLViewField(field_name="field_name", parameters=[]),
                    DimensionLookMLViewField(field_name="another_field", parameters=[]),
                ],
            ),
        ]
        dashboard = LookMLDashboard(
            dashboard_name="test_dashboard",
            parameters=[],
            filters=[
                LookMLDashboardFilter(name="test filter", field="view_name.field_name")
            ],
            elements=[LookMLDashboardElement(name="test element")],
        )
        try:
            dashboard.validate_referenced_fields_exist_in_views(views)
        except ValueError:
            self.fail(
                "validate_referenced_fields_exist_in_views raised ValueError unexpectedly!"
            )

    def test_validate_referenced_fields_exist_in_views_invalid_filter(self) -> None:
        views = [
            LookMLView(
                view_name="view_name",
                fields=[
                    DimensionLookMLViewField(field_name="field_name", parameters=[]),
                    DimensionLookMLViewField(field_name="another_field", parameters=[]),
                ],
            ),
        ]
        dashboard = LookMLDashboard(
            dashboard_name="test_dashboard",
            parameters=[],
            filters=[
                LookMLDashboardFilter(
                    name="test filter", field="view_name.invalid_field"
                )
            ],
            elements=[LookMLDashboardElement(name="test element")],
        )
        with self.assertRaises(ValueError):
            dashboard.validate_referenced_fields_exist_in_views(views)
