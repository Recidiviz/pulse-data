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
"""Tests for YAML utils."""
import unittest

from recidiviz.utils.yaml import get_properly_quoted_yaml_str


class TestGetProperlyQuotedYamlString(unittest.TestCase):
    """Tests for get_properly_quoted_yaml_str."""

    def test_reserved_words(self) -> None:
        self.assertEqual(get_properly_quoted_yaml_str("y"), '"y"')
        self.assertEqual(get_properly_quoted_yaml_str("Yes"), '"Yes"')
        self.assertEqual(get_properly_quoted_yaml_str("TRUE"), '"TRUE"')
        self.assertEqual(
            f"value: {get_properly_quoted_yaml_str('123')}", 'value: "123"'
        )
        self.assertEqual(
            f"value: {get_properly_quoted_yaml_str('123.4')}", 'value: "123.4"'
        )
        self.assertEqual(f"value: {get_properly_quoted_yaml_str('??')}", 'value: "??"')
        self.assertEqual(
            f"value: {get_properly_quoted_yaml_str('emily is #1')}",
            'value: "emily is #1"',
        )

    def test_not_reserved_words(self) -> None:
        self.assertEqual(get_properly_quoted_yaml_str("foo"), "foo")
        self.assertEqual(get_properly_quoted_yaml_str("This is true"), "This is true")
        self.assertEqual(get_properly_quoted_yaml_str("123a"), "123a")
        self.assertEqual(get_properly_quoted_yaml_str("2<3"), "2<3")
        self.assertEqual(
            get_properly_quoted_yaml_str("foo", always_quote=True), '"foo"'
        )

    def test_escape_quotes(self) -> None:
        self.assertEqual(get_properly_quoted_yaml_str('foo"bar'), 'foo"bar')
        self.assertEqual(
            get_properly_quoted_yaml_str('foo"bar', always_quote=True), '"foo\\"bar"'
        )
