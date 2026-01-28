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
"""Tests for recidiviz/utils/string.py"""
import unittest

from recidiviz.utils.string import is_meaningful_docstring


class TestIsMeaningfulDocstring(unittest.TestCase):
    """Tests for is_meaningful_docstring"""

    def test_is_meaningful_docstring(self) -> None:
        # These are not meaningful
        self.assertFalse(is_meaningful_docstring(None))
        self.assertFalse(is_meaningful_docstring(""))
        self.assertFalse(is_meaningful_docstring("XXXX"))
        self.assertFalse(is_meaningful_docstring("TO" + "DO(#123): Fill this out"))
        self.assertFalse(is_meaningful_docstring("   TO" + "DO(#123): Fill this out"))

        # These are meaningful
        self.assertTrue(is_meaningful_docstring("This columns does X"))
        self.assertTrue(
            is_meaningful_docstring(
                "This columns does X. TO" + "DO(#123): Ask clarifying question Y"
            )
        )
