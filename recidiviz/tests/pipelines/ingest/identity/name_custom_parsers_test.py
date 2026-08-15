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
"""Tests for the name custom parsers."""
import unittest

from recidiviz.pipelines.ingest.identity.name_custom_parsers import (
    clean_name_part_or_null,
    clean_name_suffix_or_null,
)


class TestCleanNamePartOrNull(unittest.TestCase):
    """Tests clean_name_part_or_null."""

    def test_valid_name_passes_through_unchanged(self) -> None:
        self.assertEqual("O'BRIEN-MCGRATH", clean_name_part_or_null("O'BRIEN-MCGRATH"))
        self.assertEqual("JOSÉ", clean_name_part_or_null("JOSÉ"))

    def test_decomposed_accent_normalized_to_precomposed(self) -> None:
        # NFC normalization turns a base letter plus combining mark into the
        # precomposed letter, rather than the mark being deleted as a
        # disallowed character ("JOSE" with the accent lost). Escaped
        # rather than literal because the encodings look identical in
        # source.
        self.assertEqual("JOS\u00c9", clean_name_part_or_null("JOSE\u0301"))

    def test_disallowed_characters_stripped(self) -> None:
        self.assertEqual("JOHN PAUL", clean_name_part_or_null("JOHN*  PAUL"))
        self.assertEqual("NO GANG NAME", clean_name_part_or_null("NO GANG NAME ?"))
        self.assertEqual("SMITH JR", clean_name_part_or_null("SMITH, JR"))

    def test_digits_stripped_from_name_part(self) -> None:
        self.assertEqual("SMITH", clean_name_part_or_null("SMITH 2"))

    def test_disallowed_character_between_letters_deleted_not_spaced(self) -> None:
        # Deleting (rather than space-replacing) a disallowed character is
        # deliberate. Junk between letters in the raw data is almost always a
        # mistyped letter (a zero for an O), where deletion is the closer
        # repair; separator junk nearly always has adjacent whitespace, where
        # the two behave identically ("SMITH, JR" above).
        self.assertEqual("JHNSON", clean_name_part_or_null("J0HNSON"))
        self.assertEqual("SMITHJR", clean_name_part_or_null("SMITH,JR"))

    def test_whitespace_normalized(self) -> None:
        self.assertEqual("ANNA MARIA", clean_name_part_or_null("  ANNA \t MARIA  "))

    def test_nothing_usable_left_is_null(self) -> None:
        self.assertIsNone(clean_name_part_or_null("*"))
        self.assertIsNone(clean_name_part_or_null("123"))
        self.assertIsNone(clean_name_part_or_null("   "))

    def test_empty_is_null(self) -> None:
        self.assertIsNone(clean_name_part_or_null(""))


class TestCleanNameSuffixOrNull(unittest.TestCase):
    """Tests clean_name_suffix_or_null."""

    def test_valid_suffix_passes_through_unchanged(self) -> None:
        self.assertEqual("JR.", clean_name_suffix_or_null("JR."))
        self.assertEqual("Jr, MD", clean_name_suffix_or_null("Jr, MD"))

    def test_digit_bearing_suffixes_survive(self) -> None:
        self.assertEqual("3RD", clean_name_suffix_or_null("3RD"))
        self.assertEqual("2", clean_name_suffix_or_null("2"))

    def test_disallowed_characters_stripped(self) -> None:
        self.assertEqual("3RD", clean_name_suffix_or_null("(3RD)"))
        self.assertEqual("JR", clean_name_suffix_or_null("JR*"))

    def test_over_length_result_is_null(self) -> None:
        self.assertIsNone(clean_name_suffix_or_null("JUNIOR ESQUIRE III"))

    def test_nothing_usable_left_is_null(self) -> None:
        self.assertIsNone(clean_name_suffix_or_null("(?)"))
        self.assertIsNone(clean_name_suffix_or_null("   "))

    def test_empty_is_null(self) -> None:
        self.assertIsNone(clean_name_suffix_or_null(""))
