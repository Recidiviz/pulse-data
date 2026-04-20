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
"""Tests for recidiviz_attr_validators.py."""
import unittest

import attr

from recidiviz.common import recidiviz_attr_validators as attr_validators


class TestIsMeaningfulDescription(unittest.TestCase):
    """Tests for the is_meaningful_description() validator."""

    @attr.define
    class _TestClass:
        description: str = attr.ib(validator=attr_validators.is_meaningful_description)

    def test_valid_description(self) -> None:
        _ = self._TestClass(description="A real description of the column")

    def test_empty_string(self) -> None:
        with self.assertRaisesRegex(ValueError, "must have a meaningful description"):
            _ = self._TestClass(description="")

    def test_placeholder_todo(self) -> None:
        with self.assertRaisesRegex(ValueError, "must have a meaningful description"):
            _ = self._TestClass(description="TODO(#000) fill this in later")

    def test_placeholder_xxx(self) -> None:
        with self.assertRaisesRegex(ValueError, "must have a meaningful description"):
            _ = self._TestClass(description="XXX placeholder")

    def test_non_string_type(self) -> None:
        with self.assertRaises((TypeError, ValueError)):
            _ = self._TestClass(description=123)  # type: ignore[arg-type]
