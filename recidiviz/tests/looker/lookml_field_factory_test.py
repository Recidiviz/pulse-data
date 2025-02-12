# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for LookMLFieldFactory."""
import unittest

from recidiviz.looker.lookml_field_factory import LookMLFieldFactory


class TestLookMLFieldFactory(unittest.TestCase):
    """Tests for LookMLFieldFactory."""

    def test_count_measure(self) -> None:
        expected_field = """
  measure: count {
    type: count
    drill_fields: []
  }"""
        field = LookMLFieldFactory.count_measure()
        self.assertEqual(field.build(), expected_field)

    def test_count_measure_with_drill_fields(self) -> None:
        expected_field = """
  measure: count {
    type: count
    drill_fields: [field1, field2]
  }"""
        field = LookMLFieldFactory.count_measure(drill_fields=["field1", "field2"])
        self.assertEqual(field.build(), expected_field)

    def test_count_field_measure(self) -> None:
        expected_field = """
  measure: count_test_field {
    type: number
    sql: COUNT(${test_field}) ;;
  }"""
        field = LookMLFieldFactory.count_field_measure("test_field")
        self.assertEqual(field.build(), expected_field)

    def test_average_measure(self) -> None:
        expected_field = """
  measure: average_test_field {
    type: average
    sql: ${test_field} ;;
  }"""
        field = LookMLFieldFactory.average_measure("test_field")
        self.assertEqual(field.build(), expected_field)

    def test_sum_measure(self) -> None:
        expected_field = """
  measure: total_test_field {
    type: sum
    sql: ${test_field} ;;
  }"""
        field = LookMLFieldFactory.sum_measure("test_field")
        self.assertEqual(field.build(), expected_field)
