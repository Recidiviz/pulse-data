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
    LookMLDashboardElement,
    LookMLElementType,
    LookMLListen,
    LookMLNoteDisplayType,
)


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
            sorts=["view1.field1 asc", "view2.field2 desc", "view3.field3"],
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
    sorts: [view1.field1 asc,
      view2.field2 desc,
      view3.field3]
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
