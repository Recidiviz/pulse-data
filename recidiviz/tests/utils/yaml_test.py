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

import yaml

from recidiviz.utils.yaml import (
    get_properly_quoted_yaml_str,
    prettier_friendly_yaml_dump,
)


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


class TestPrettierFriendlyYamlDump(unittest.TestCase):
    """Tests for prettier_friendly_yaml_dump."""

    def test_block_sequences_are_indented_under_their_key(self) -> None:
        self.assertEqual(
            prettier_friendly_yaml_dump({"roles": ["a", "b"]}),
            "roles:\n  - a\n  - b\n",
        )

    def test_strings_needing_quotes_are_double_quoted(self) -> None:
        self.assertEqual(
            prettier_friendly_yaml_dump({"description": "Contains things: and more"}),
            'description: "Contains things: and more"\n',
        )
        self.assertEqual(
            prettier_friendly_yaml_dump({"value": "true"}), 'value: "true"\n'
        )

    def test_strings_with_embedded_double_quotes_are_single_quoted(self) -> None:
        # The trailing space forces quoting; the embedded double quotes make
        # prettier prefer single quotes.
        self.assertEqual(
            prettier_friendly_yaml_dump({"cmd": 'echo "$${VAR}" && run '}),
            "cmd: 'echo \"$${VAR}\" && run '\n",
        )

    def test_strings_with_more_single_than_double_quotes_are_double_quoted(
        self,
    ) -> None:
        self.assertEqual(
            prettier_friendly_yaml_dump({"cmd": "echo 'hi': done"}),
            "cmd: \"echo 'hi': done\"\n",
        )

    def test_multiline_strings_are_double_quoted_with_escapes(self) -> None:
        self.assertEqual(
            prettier_friendly_yaml_dump({"cmd": 'set -eu\necho "$${VAR}"'}),
            'cmd: "set -eu\\necho \\"$${VAR}\\""\n',
        )

    def test_plain_strings_stay_unquoted(self) -> None:
        self.assertEqual(
            prettier_friendly_yaml_dump({"description": "Stores things"}),
            "description: Stores things\n",
        )

    def test_long_strings_are_not_wrapped(self) -> None:
        long_value = "word " * 100
        dumped = prettier_friendly_yaml_dump({"description": long_value.strip()})
        self.assertEqual(len(dumped.splitlines()), 1)

    def test_keys_keep_insertion_order(self) -> None:
        self.assertEqual(prettier_friendly_yaml_dump({"b": 1, "a": 2}), "b: 1\na: 2\n")

    def test_round_trips_through_safe_load(self) -> None:
        value = {
            "datasets": {
                "my_dataset": {
                    "description": "Contains things: and more",
                    "default_table_expiration_ms": 604800000,
                    "projects": ["recidiviz-staging"],
                }
            }
        }
        self.assertEqual(yaml.safe_load(prettier_friendly_yaml_dump(value)), value)
