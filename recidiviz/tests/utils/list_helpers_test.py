# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for list_helpers.py."""
import unittest

from recidiviz.utils.list_helpers import flatten_values_for_keys, group_by, index_by


class TestGroupBy(unittest.TestCase):
    """Tests for group_by."""

    def test_group_by(self) -> None:
        self.assertEqual(
            {0: [2, 4], 1: [1, 3]},
            group_by([1, 2, 3, 4], key_fn=lambda n: n % 2),
        )

    def test_group_by_empty(self) -> None:
        self.assertEqual({}, group_by([], key_fn=lambda n: n))


class TestIndexBy(unittest.TestCase):
    """Tests for index_by."""

    def test_index_by(self) -> None:
        self.assertEqual(
            {"a": "apple", "b": "banana"},
            index_by(["apple", "banana"], key_fn=lambda s: s[0]),
        )

    def test_index_by_empty(self) -> None:
        self.assertEqual({}, index_by([], key_fn=lambda n: n))

    def test_index_by_duplicate_key_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Found more than one item with key \[a\]$"
        ):
            index_by(["apple", "avocado"], key_fn=lambda s: s[0])


class TestFlattenValuesForKeys(unittest.TestCase):
    """Tests for flatten_values_for_keys."""

    def test_flatten_values_for_keys(self) -> None:
        # Preserves key order and concatenates each key's list, including a repeated key.
        self.assertEqual(
            [1, 2, 5, 6, 1, 2],
            flatten_values_for_keys(
                ["a", "c", "a"], {"a": [1, 2], "b": [3, 4], "c": [5, 6]}
            ),
        )

    def test_flatten_values_for_keys_empty_keys(self) -> None:
        self.assertEqual([], flatten_values_for_keys([], {"a": [1, 2]}))

    def test_flatten_values_for_keys_missing_key_raises(self) -> None:
        with self.assertRaisesRegex(KeyError, r"No values found for key \[b\]"):
            flatten_values_for_keys(["a", "b"], {"a": [1, 2]})
