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
"""Tests for InMemoryExpectedEntryNumsSource."""
from unittest import TestCase

from recidiviz.documents.extraction.expected_entry_nums_source import (
    InMemoryExpectedEntryNumsSource,
)


class InMemoryExpectedEntryNumsSourceTest(TestCase):
    """Tests for InMemoryExpectedEntryNumsSource."""

    def test_returns_entry_set_for_known_document(self) -> None:
        source = InMemoryExpectedEntryNumsSource(
            entry_nums_by_document={"COMPOSITE_A": {1, 2}, "COMPOSITE_B": {3}}
        )

        self.assertEqual(
            {1, 2}, source.get_expected_entry_nums(document_contents_id="COMPOSITE_A")
        )
        self.assertEqual(
            {3}, source.get_expected_entry_nums(document_contents_id="COMPOSITE_B")
        )

    def test_unknown_document_raises(self) -> None:
        source = InMemoryExpectedEntryNumsSource(
            entry_nums_by_document={"COMPOSITE_A": {1, 2}}
        )

        with self.assertRaises(KeyError):
            source.get_expected_entry_nums(document_contents_id="UNKNOWN")
