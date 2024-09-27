# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for string_formatting.py"""
import unittest

from recidiviz.utils.string_formatting import fix_indent


class TestFixIndent(unittest.TestCase):
    """Tests for string_formatting.py"""

    def test_fix_indent_noop(self) -> None:
        s = """No indent"""
        self.assertEqual(s, fix_indent(s, indent_level=0))

        s = """  Same indent"""
        self.assertEqual(s, fix_indent(s, indent_level=2))

        s = """  Same indent
  Multiline"""
        self.assertEqual(s, fix_indent(s, indent_level=2))

    def test_fix_indent_increase_indent(self) -> None:
        s = """No indent"""
        expected_s = """    No indent"""
        self.assertEqual(expected_s, fix_indent(s, indent_level=4))

        s = """  Small indent"""
        expected_s = """    Small indent"""
        self.assertEqual(expected_s, fix_indent(s, indent_level=4))

        s = """  Small indent
    Second line different indent"""
        expected_s = """    Small indent
      Second line different indent"""
        self.assertEqual(expected_s, fix_indent(s, indent_level=4))

    def test_fix_indent_decrease_indent(self) -> None:
        s = """    Large indent"""
        expected_s = """  Large indent"""
        self.assertEqual(expected_s, fix_indent(s, indent_level=2))

        s = """    Large indent"""
        expected_s = """Large indent"""
        self.assertEqual(expected_s, fix_indent(s, indent_level=0))

        s = """    Large indent
      Second line different indent"""
        expected_s = """  Large indent
    Second line different indent"""
        self.assertEqual(expected_s, fix_indent(s, indent_level=2))

    def test_fix_indent_complex(self) -> None:
        s = """
    SELECT *
    FROM (
        SELECT * FROM table
    )
        """
        expected_s = """  SELECT *
  FROM (
      SELECT * FROM table
  )"""
        self.assertEqual(expected_s, fix_indent(s, indent_level=2))

        expected_s = """SELECT *
FROM (
    SELECT * FROM table
)"""
        self.assertEqual(expected_s, fix_indent(s, indent_level=0))
