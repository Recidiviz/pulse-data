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
"""Unit tests for the Edovo learner name matching rules."""
from unittest import TestCase

from recidiviz.case_triage.edovo.name_matching import (
    given_names_match,
    normalized_name,
    surnames_match,
)


class TestNormalizedName(TestCase):
    """Tests for reducing a name to its comparable core."""

    def test_uppercases(self) -> None:
        self.assertEqual("JANE", normalized_name("jane"))

    def test_drops_punctuation_and_spaces(self) -> None:
        self.assertEqual("OBRIENDOE", normalized_name("O'Brien-Doe"))

    def test_folds_accents_to_ascii(self) -> None:
        self.assertEqual("NUNEZ", normalized_name("Núñez"))

    def test_drops_digits(self) -> None:
        self.assertEqual("DOE", normalized_name("Doe 2"))

    def test_returns_empty_for_a_name_with_no_letters(self) -> None:
        self.assertEqual("", normalized_name("--"))

    def test_returns_empty_for_an_empty_name(self) -> None:
        self.assertEqual("", normalized_name(""))


class TestGivenNamesMatch(TestCase):
    """Tests for comparing a submitted first name to stored given names."""

    def test_matches_identical_names(self) -> None:
        self.assertTrue(
            given_names_match(submitted_first_name="Jane", stored_given_names="JANE")
        )

    def test_matches_when_we_store_a_middle_name_edovo_omits(self) -> None:
        self.assertTrue(
            given_names_match(
                submitted_first_name="Jane", stored_given_names="JANE MARIE"
            )
        )

    def test_matches_when_edovo_sends_a_middle_name_we_omit(self) -> None:
        self.assertTrue(
            given_names_match(
                submitted_first_name="Jane Marie", stored_given_names="JANE"
            )
        )

    def test_matches_across_punctuation(self) -> None:
        self.assertTrue(
            given_names_match(
                submitted_first_name="Mary-Jane", stored_given_names="MARY JANE"
            )
        )

    def test_does_not_match_a_different_first_name(self) -> None:
        self.assertFalse(
            given_names_match(submitted_first_name="Jane", stored_given_names="ROBERT")
        )

    def test_agrees_when_we_hold_no_given_names(self) -> None:
        """An incomplete record on our side is not identifier drift on Edovo's,
        so a name we do not hold cannot disagree with the one they sent."""
        self.assertTrue(
            given_names_match(submitted_first_name="Jane", stored_given_names="")
        )

    def test_agrees_when_our_given_names_hold_no_letters(self) -> None:
        """A stored placeholder is no more a name than an empty string is."""
        self.assertTrue(
            given_names_match(submitted_first_name="Jane", stored_given_names="--")
        )

    def test_does_not_match_two_names_whose_first_tokens_normalize_away(
        self,
    ) -> None:
        """Both first tokens reduce to nothing, so comparing them would match
        Jane to Robert."""
        self.assertFalse(
            given_names_match(
                submitted_first_name="-- Jane", stored_given_names="-- Robert"
            )
        )

    def test_does_not_match_two_names_behind_different_digit_prefixes(self) -> None:
        self.assertFalse(
            given_names_match(
                submitted_first_name="123 Jane", stored_given_names="456 Robert"
            )
        )

    def test_does_not_match_a_middle_name_against_a_first_name(self) -> None:
        """Only the first token is compared, so Edovo's first name must be our
        first name — matching it against our middle name would let two
        different people compare equal."""
        self.assertFalse(
            given_names_match(
                submitted_first_name="Marie", stored_given_names="JANE MARIE"
            )
        )


class TestSurnamesMatch(TestCase):
    """Tests for comparing a submitted last name to a stored surname."""

    def test_matches_identical_surnames(self) -> None:
        self.assertTrue(surnames_match(submitted_last_name="Doe", stored_surname="DOE"))

    def test_matches_compound_surname_written_differently(self) -> None:
        self.assertTrue(
            surnames_match(
                submitted_last_name="van der Berg", stored_surname="VAN-DER-BERG"
            )
        )

    def test_does_not_match_a_different_surname(self) -> None:
        self.assertFalse(
            surnames_match(submitted_last_name="Doe", stored_surname="SMITH")
        )

    def test_agrees_when_we_hold_no_surname(self) -> None:
        self.assertTrue(surnames_match(submitted_last_name="Doe", stored_surname=""))

    def test_does_not_match_a_partial_surname(self) -> None:
        """The whole surname is compared, so a prefix of ours is not a match."""
        self.assertFalse(
            surnames_match(submitted_last_name="Van", stored_surname="VAN DER BERG")
        )
