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
"""Tests functionality of LookMLDashboard element"""
import unittest

from recidiviz.looker.lookml_dashboard_element import (
    LookMLColorApplication,
    LookMLDashboardElement,
    LookMLElementType,
    LookMLListen,
    LookMLNoteDisplayType,
    LookMLSort,
    build_dashboard_grid,
)
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import DimensionLookMLViewField


class LookMLDashboardElementTest(unittest.TestCase):
    """
    Tests LookMLDashboardElement
    """

    def test_dashboard_element_no_attributes(self) -> None:
        # Test a element without attributes
        element_output = LookMLDashboardElement(name="test element").build()
        expected_result = "- name: test element"
        self.assertEqual(element_output, expected_result)

    def test_dashboard_element_many_attributes(self) -> None:
        # Test a element with many attributes filled in
        element_output = LookMLDashboardElement(
            name="test name",
            title="test title",
            explore="test explore",
            model="test model",
            type=LookMLElementType.LOOKER_GRID,
            fields=["view1.field1", "view2.field2", "view3.field3"],
            sorts=[
                LookMLSort("view1.field1"),
                LookMLSort("view2.field2", desc=True),
                LookMLSort("view3.field3"),
            ],
            note_display=LookMLNoteDisplayType.ABOVE,
            note_text="|-\ntest note text",
            listen=LookMLListen({"FILTER1": "view1.field1", "FILTER2": "view2.field2"}),
            row=1,
            col=2,
            width=3,
            height=4,
        ).build()
        expected_result = """- name: test name
    title: test title
    explore: test explore
    model: test model
    type: looker_grid
    fields: [view1.field1,
      view2.field2,
      view3.field3]
    sorts: [view1.field1, view2.field2 desc, view3.field3]
    note_display: above
    note_text: |-
test note text
    listen: 
      FILTER1: view1.field1
      FILTER2: view2.field2
    row: 1
    col: 2
    width: 3
    height: 4"""
        self.assertEqual(element_output, expected_result)

    def test_validate_referenced_fields_exist_in_views_valid(self) -> None:
        element = LookMLDashboardElement(
            name="test element", fields=["view1.field1", "view2.field2"]
        )
        views = [
            LookMLView(
                view_name="view1",
                fields=[
                    DimensionLookMLViewField(field_name="field1", parameters=[]),
                    DimensionLookMLViewField(field_name="field3", parameters=[]),
                ],
            ),
            LookMLView(
                view_name="view2",
                fields=[
                    DimensionLookMLViewField(field_name="field2", parameters=[]),
                    DimensionLookMLViewField(field_name="field4", parameters=[]),
                ],
            ),
        ]
        # Should not raise an error
        element.validate_referenced_fields_exist_in_views(views)

    def test_validate_referenced_fields_exist_in_views_invalid_view(self) -> None:
        element = LookMLDashboardElement(
            name="test element", fields=["view1.field1", "view3.field2"]
        )
        views = [
            LookMLView(
                view_name="view1",
                fields=[
                    DimensionLookMLViewField(field_name="field1", parameters=[]),
                    DimensionLookMLViewField(field_name="field3", parameters=[]),
                ],
            ),
            LookMLView(
                view_name="view2",
                fields=[
                    DimensionLookMLViewField(field_name="field2", parameters=[]),
                    DimensionLookMLViewField(field_name="field4", parameters=[]),
                ],
            ),
        ]
        with self.assertRaises(ValueError) as context:
            element.validate_referenced_fields_exist_in_views(views)
        self.assertEqual(str(context.exception), "View [view3] is not defined.")

    def test_color_application(self) -> None:
        color_application = LookMLColorApplication(
            collection_id="test_collection", palette_id="test_palette"
        )
        expected_result = """
      collection_id: test_collection
      palette_id: test_palette"""
        self.assertEqual(color_application.build(), expected_result)

    def test_sort(self) -> None:
        sort = LookMLSort(field="view1.field1", desc=True)
        expected_result = "view1.field1 desc"
        self.assertEqual(sort.build(), expected_result)

    def test_build_dashboard_grid_custom_dimensions(self) -> None:
        elements = [
            LookMLDashboardElement(name="element1", width=8, height=4),
            LookMLDashboardElement(name="element2", width=12, height=4),
            LookMLDashboardElement(name="element3", width=16, height=4),
        ]
        build_dashboard_grid(elements)
        self.assertEqual(elements[0].row, 0)
        self.assertEqual(elements[0].col, 0)
        self.assertEqual(elements[1].row, 0)
        self.assertEqual(elements[1].col, 8)
        self.assertEqual(elements[2].row, 4)
        self.assertEqual(elements[2].col, 0)

    def test_build_dashboard_grid_row_set(self) -> None:
        elements = [
            LookMLDashboardElement(name="element1", width=8, height=4),
            LookMLDashboardElement(name="element2", width=12, height=4, row=4),
            LookMLDashboardElement(name="element3", width=16, height=4),
        ]
        with self.assertRaises(ValueError) as context:
            build_dashboard_grid(elements)
        self.assertEqual(
            str(context.exception),
            "Element [element2] already has a row or column value set.",
        )
