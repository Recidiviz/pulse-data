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

from recidiviz.utils.string_formatting import (
    collapse_blank_lines,
    collapse_whitespace,
    fix_indent,
    render_list,
    truncate_string_if_necessary,
)


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


class TestCollapseWhitespace(unittest.TestCase):
    """Tests for collapse_whitespace."""

    def test_collapses_runs_including_newlines_and_strips(self) -> None:
        self.assertEqual(
            "one two three",
            collapse_whitespace("  one   two\n\tthree  "),
        )

    def test_already_collapsed_is_unchanged(self) -> None:
        self.assertEqual("one two three", collapse_whitespace("one two three"))

    def test_empty_string(self) -> None:
        self.assertEqual("", collapse_whitespace("   \n  "))


class TestCollapseBlankLines(unittest.TestCase):
    """Tests for collapse_blank_lines."""

    def test_collapses_runs_of_blank_lines_to_one(self) -> None:
        source = """\
a




b"""
        expected = """\
a

b"""
        self.assertEqual(expected, collapse_blank_lines(source))

    def test_single_blank_line_is_preserved(self) -> None:
        text = """\
a

b"""
        self.assertEqual(text, collapse_blank_lines(text))

    def test_strips_leading_and_trailing_whitespace(self) -> None:
        source = """

a


b

"""
        expected = """\
a

b"""
        self.assertEqual(expected, collapse_blank_lines(source))


class TestRenderList(unittest.TestCase):
    """Tests for render_list."""

    def test_single_line_items(self) -> None:
        expected = """\
- a
- b"""
        self.assertEqual(expected, render_list(["a", "b"]))

    def test_empty_iterable(self) -> None:
        self.assertEqual("", render_list([]))

    def test_multi_line_item_hanging_indents_continuations(self) -> None:
        expected = """\
- first line
  second line
  third line"""
        self.assertEqual(expected, render_list(["first line\nsecond line\nthird line"]))

    def test_blank_continuation_line_becomes_empty(self) -> None:
        # A blank line inside an item must not carry hanging-indent whitespace.
        expected = """\
- first line

  third line"""
        self.assertEqual(expected, render_list(["first line\n\nthird line"]))

    def test_custom_bullet_char_widens_hanging_indent(self) -> None:
        expected = """\
* first line
  second line"""
        self.assertEqual(
            expected, render_list(["first line\nsecond line"], bullet_char="*")
        )

    def test_indent_level_reindents_whole_list(self) -> None:
        expected = """\
    - a
    - b"""
        self.assertEqual(expected, render_list(["a", "b"], indent_level=4))


class TestTruncateString(unittest.TestCase):
    def test_no_truncation(self) -> None:
        self.assertEqual("abc123", truncate_string_if_necessary("abc123", max_length=6))
        self.assertEqual(
            "abc123", truncate_string_if_necessary("abc123", max_length=10)
        )

    def test_truncate(self) -> None:
        s = (
            "99 bottles of beer on the wall, 99 bottles of beer. Take one down pass it "
            "around, 98 bottles of beer on the wall."
        )

        truncated_s = truncate_string_if_necessary(s, max_length=45)
        self.assertEqual(45, len(truncated_s))
        self.assertEqual("99 bottles of beer on the wal ... (truncated)", truncated_s)

    def test_truncate_custom(self) -> None:
        s = (
            "99 bottles of beer on the wall, 99 bottles of beer. Take one down pass it "
            "around, 98 bottles of beer on the wall."
        )

        truncated_s = truncate_string_if_necessary(
            s, max_length=45, truncation_message="[TRUNCATED]"
        )
        self.assertEqual(45, len(truncated_s))
        self.assertEqual("99 bottles of beer on the wall, 99[TRUNCATED]", truncated_s)
