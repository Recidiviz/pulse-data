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
"""Tests for the shared name suffix vocabulary."""
import unittest

from recidiviz.common.constants.name_suffixes import (
    CANONICAL_NAME_SUFFIXES,
    EMBEDDED_NAME_SUFFIX_TOKENS,
)


class TestNameSuffixes(unittest.TestCase):
    """Tests for the shared name suffix vocabulary."""

    def test_embedded_tokens_are_recognized_suffixes(self) -> None:
        unrecognized_tokens = set(EMBEDDED_NAME_SUFFIX_TOKENS) - set(
            CANONICAL_NAME_SUFFIXES
        )
        self.assertFalse(
            unrecognized_tokens,
            f"Embedded suffix tokens [{sorted(unrecognized_tokens)}] are not "
            f"recognized suffixes in CANONICAL_NAME_SUFFIXES.",
        )

    def test_canonical_forms_are_themselves_canonical(self) -> None:
        for suffix, canonical_form in CANONICAL_NAME_SUFFIXES.items():
            self.assertEqual(
                canonical_form,
                CANONICAL_NAME_SUFFIXES[canonical_form],
                f"Canonical form [{canonical_form}] of suffix [{suffix}] does "
                f"not canonicalize to itself.",
            )
