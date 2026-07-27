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
"""Tests for nickname_equivalence."""
import csv
import os
import unittest

from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking import (
    nickname_equivalence,
)
from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking.nickname_equivalence import (
    are_names_nickname_equivalent,
)


class TestAreNamesNicknameEquivalent(unittest.TestCase):
    """Tests for are_names_nickname_equivalent."""

    def test_nickname_and_canonical_are_equivalent(self) -> None:
        self.assertTrue(are_names_nickname_equivalent("mike", "michael"))
        self.assertTrue(are_names_nickname_equivalent("bob", "robert"))
        self.assertTrue(are_names_nickname_equivalent("tony", "anthony"))

    def test_two_nicknames_of_one_canonical_are_equivalent(self) -> None:
        # Both map to the canonical RICHARD.
        self.assertTrue(are_names_nickname_equivalent("richie", "ritchie"))

    def test_shared_canonical_bridges_two_of_its_nicknames(self) -> None:
        """Two nicknames of the same canonical are equivalent by design: a
        person named Bradford may be recorded as BRAD in one system and FORD
        in another. The accepted cost is that a genuine Brad and a genuine
        Ford (with matching surname and birthdate, so usually twins) would
        not conflict."""
        self.assertTrue(are_names_nickname_equivalent("brad", "ford"))

    def test_matching_is_case_insensitive(self) -> None:
        self.assertTrue(are_names_nickname_equivalent("MIKE", "Michael"))

    def test_unrelated_names_are_not_equivalent(self) -> None:
        self.assertFalse(are_names_nickname_equivalent("bob", "michael"))

    def test_equivalence_is_not_transitive(self) -> None:
        """A transitive union of the data set chains nearly every name into
        one giant component; the shared-canonical lookup must not. MELISSA and
        JONATHON both appear in the data set but share no canonical."""
        self.assertFalse(are_names_nickname_equivalent("melissa", "jonathon"))

    def test_name_absent_from_data_set_is_equivalent_to_nothing(self) -> None:
        self.assertFalse(are_names_nickname_equivalent("xzqurv", "xzqurv"))
        self.assertFalse(are_names_nickname_equivalent("xzqurv", "michael"))


class TestNicknamesCsvIsWellFormed(unittest.TestCase):
    """Enforces the nicknames.csv maintenance invariants so the file stays
    greppable and dedupable as pairs are added."""

    def _read_rows(self) -> list[list[str]]:
        path = os.path.join(
            os.path.dirname(nickname_equivalence.__file__), "nicknames.csv"
        )
        with open(path, encoding="utf-8") as f:
            return list(csv.reader(f))

    def test_header_is_expected(self) -> None:
        self.assertEqual(["canonical_name", "nickname"], self._read_rows()[0])

    def test_rows_are_two_lowercase_trimmed_cells(self) -> None:
        for row in self._read_rows()[1:]:
            self.assertEqual(2, len(row), row)
            for cell in row:
                self.assertTrue(cell, row)
                self.assertEqual(cell.strip().lower(), cell, row)

    def test_rows_are_sorted_and_deduped(self) -> None:
        body = [tuple(row) for row in self._read_rows()[1:]]
        self.assertEqual(sorted(set(body)), body)
