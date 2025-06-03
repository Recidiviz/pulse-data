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
"""Tests for ImmutableKeyDict"""
import unittest

from recidiviz.utils.immutable_key_dict import ImmutableKeyDict


class TestImmutableKeyDict(unittest.TestCase):
    """Tests for ImmutableKeyDict"""

    def test_updates_no_conflicts(self) -> None:
        d: dict[str, int] = ImmutableKeyDict()
        self.assertIsInstance(d, dict)

        d["A"] = 1
        d["B"] = 2
        d.update({"C": 3, "D": 4})
        d.update({"E": 5})
        d.update([("F", 6), ("H", 7)])

        self.assertEqual({"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6, "H": 7}, d)

        # We can explicitly remove a key and then re-add a different one
        d.pop("B")
        d["B"] = 10

        self.assertEqual({"A": 1, "B": 10, "C": 3, "D": 4, "E": 5, "F": 6, "H": 7}, d)

    def test_updates_values_conflict(self) -> None:
        d: dict[str, str] = ImmutableKeyDict()
        self.assertIsInstance(d, dict)

        d["A"] = "B"
        # Should be no issue to have a key that matches a value
        d["B"] = "C"
        # Should be no issue to have a value that matches a value
        d["C"] = "C"

    def test_set_value_with_conflict(self) -> None:
        d: dict[str, int] = ImmutableKeyDict({"A": 1})
        self.assertIsInstance(d, dict)

        with self.assertRaisesRegex(
            KeyError, "Key 'A' already exists and cannot be overwritten."
        ):
            d["A"] = 2

    def test_update_with_conflict(self) -> None:
        d: dict[str, int] = ImmutableKeyDict({"A": 1})
        self.assertIsInstance(d, dict)

        with self.assertRaisesRegex(
            KeyError, "Key 'A' already exists and cannot be overwritten."
        ):
            d.update({"A": 2, "B": 2})

        with self.assertRaisesRegex(ValueError, r"kwargs not supported in update\(\)"):
            d.update(A=2)

        with self.assertRaisesRegex(
            KeyError, "Key 'A' already exists and cannot be overwritten."
        ):
            d.update([("A", 2)])
