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
"""Tests for signals."""
import datetime
import unittest

from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking.signals import (
    _normalize_name,
    are_dobs_in_conflict,
    are_given_names_in_conflict,
    are_middle_names_in_conflict,
    are_name_suffixes_in_conflict,
    are_surnames_in_conflict,
)


class TestAreSurnamesInConflict(unittest.TestCase):
    """Tests for are_surnames_in_conflict."""

    def test_spelling_drift_within_threshold_not_in_conflict(self) -> None:
        self.assertFalse(
            are_surnames_in_conflict(
                "GUSTAFSON",
                "GUSTAFSEN",
                given_names_loosely_match=False,
                dobs_match_exactly=False,
            )
        )

    def test_long_surname_two_edit_drift_not_in_conflict(self) -> None:
        self.assertFalse(
            are_surnames_in_conflict(
                "STEPHENSON",
                "STEVENSON",
                given_names_loosely_match=False,
                dobs_match_exactly=False,
            )
        )

    def test_short_surname_gets_one_edit_budget(self) -> None:
        """Two edits on a surname of five letters or fewer is a different
        name, not spelling drift: MILLER/MILLS and JONES/JAMES conflict (the
        name-change hatch can still excuse them). A single edit stays
        forgiven whatever the length, so OLSON/OLSEN merges — and so does
        HALL/HILL, an accepted cost of tolerating one-letter typos."""
        self.assertTrue(
            are_surnames_in_conflict(
                "MILLER",
                "MILLS",
                given_names_loosely_match=False,
                dobs_match_exactly=False,
            )
        )
        self.assertTrue(
            are_surnames_in_conflict(
                "JONES",
                "JAMES",
                given_names_loosely_match=False,
                dobs_match_exactly=False,
            )
        )
        self.assertFalse(
            are_surnames_in_conflict(
                "OLSON",
                "OLSEN",
                given_names_loosely_match=False,
                dobs_match_exactly=False,
            )
        )
        self.assertFalse(
            are_surnames_in_conflict(
                "HALL",
                "HILL",
                given_names_loosely_match=False,
                dobs_match_exactly=False,
            )
        )

    def test_token_subset_not_in_conflict(self) -> None:
        self.assertFalse(
            are_surnames_in_conflict(
                "LOPEZ-SMITH",
                "LOPEZ",
                given_names_loosely_match=False,
                dobs_match_exactly=False,
            )
        )

    def test_different_surnames_in_conflict(self) -> None:
        self.assertTrue(
            are_surnames_in_conflict(
                "WILLIAMS",
                "JOHNSON",
                given_names_loosely_match=False,
                dobs_match_exactly=False,
            )
        )

    def test_name_change_escape_hatch(self) -> None:
        """Different surnames are not a conflict when given name and date of
        birth both corroborate: a name change, not a different person."""
        self.assertFalse(
            are_surnames_in_conflict(
                "WILLIAMS",
                "JOHNSON",
                given_names_loosely_match=True,
                dobs_match_exactly=True,
            )
        )
        self.assertTrue(
            are_surnames_in_conflict(
                "WILLIAMS",
                "JOHNSON",
                given_names_loosely_match=True,
                dobs_match_exactly=False,
            )
        )

    def test_missing_surname_not_in_conflict(self) -> None:
        self.assertFalse(
            are_surnames_in_conflict(
                None,
                "JOHNSON",
                given_names_loosely_match=False,
                dobs_match_exactly=False,
            )
        )


class TestAreGivenNamesInConflict(unittest.TestCase):
    """Tests for are_given_names_in_conflict."""

    def test_ungated_escapes_need_no_corroboration(self) -> None:
        """Token-subset and one-character drift can only equate forms of the
        same name, so they are trusted on the strings alone."""
        for name_a, name_b in [
            ("ANNE MARIE", "ANNE"),  # token subset
            ("JON", "JOHN"),  # one-character drift
        ]:
            with self.subTest(name_a=name_a, name_b=name_b):
                self.assertFalse(
                    are_given_names_in_conflict(
                        name_a,
                        name_b,
                        surnames_match_exactly=False,
                        dobs_match_exactly=False,
                    )
                )

    def test_one_character_drift_forgiven_without_corroboration(self) -> None:
        """Accepted cost of the ungated one-character rule: two given names one
        edit apart are treated as the same name even with no corroboration,
        because a one-edit difference is indistinguishable from a typo.
        """
        self.assertFalse(
            are_given_names_in_conflict(
                "ERIC",
                "ERICA",
                surnames_match_exactly=False,
                dobs_match_exactly=False,
            )
        )

    def test_nickname_requires_corroboration(self) -> None:
        """A nickname pair shares few letters, so it is trusted only when
        surname and date of birth corroborate. A short form (EARL/EARLWIN)
        lives in the nickname data set rather than matching by a bare prefix,
        so it follows the same corroboration rule."""
        for name_a, name_b in [("BOB", "ROBERT"), ("EARLWIN", "EARL")]:
            with self.subTest(name_a=name_a, name_b=name_b):
                self.assertFalse(
                    are_given_names_in_conflict(
                        name_a,
                        name_b,
                        surnames_match_exactly=True,
                        dobs_match_exactly=True,
                    )
                )
                self.assertTrue(
                    are_given_names_in_conflict(
                        name_a,
                        name_b,
                        surnames_match_exactly=False,
                        dobs_match_exactly=False,
                    )
                )

    def test_two_character_drift_requires_corroboration(self) -> None:
        self.assertFalse(
            are_given_names_in_conflict(
                "JAIME",
                "JAMIE",
                surnames_match_exactly=True,
                dobs_match_exactly=True,
            )
        )
        self.assertTrue(
            are_given_names_in_conflict(
                "JAIME",
                "JAMIE",
                surnames_match_exactly=False,
                dobs_match_exactly=False,
            )
        )

    def test_corroborated_drift_forgives_twins(self) -> None:
        """Accepted cost of the corroborated two-character escape: twins always
        share surname and exact birthdate, so they supply the corroboration the
        gate asks for, and a two-edit name difference like BRAD/CHAD is forgiven
        as spelling drift. Sex discrimination catches the opposite-sex pairs
        (when enabled); same-sex twins are beyond attribute checks."""
        self.assertFalse(
            are_given_names_in_conflict(
                "BRAD",
                "CHAD",
                surnames_match_exactly=True,
                dobs_match_exactly=True,
            )
        )

    def test_genuinely_different_names_in_conflict_despite_corroboration(
        self,
    ) -> None:
        self.assertTrue(
            are_given_names_in_conflict(
                "DEANNA",
                "DIANE",
                surnames_match_exactly=True,
                dobs_match_exactly=True,
            )
        )

    def test_name_that_normalizes_to_empty_is_treated_as_missing(self) -> None:
        """A name with no usable signal after normalization (no Latin letters,
        or nothing but a parenthetical alias) is treated like a missing name:
        it never conflicts with anything. This is the conservative failure
        direction: an empty normalization can never cause a false skip, it just
        contributes no protection for the pair."""
        for empty_after_normalization in ["李明", "()"]:
            with self.subTest(name=empty_after_normalization):
                self.assertEqual("", _normalize_name(empty_after_normalization))
                self.assertFalse(
                    are_given_names_in_conflict(
                        empty_after_normalization,
                        "MICHAEL",
                        surnames_match_exactly=False,
                        dobs_match_exactly=False,
                    )
                )

    def test_missing_name_not_in_conflict(self) -> None:
        self.assertFalse(
            are_given_names_in_conflict(
                None, "JOHN", surnames_match_exactly=False, dobs_match_exactly=False
            )
        )


class TestAreMiddleNamesInConflict(unittest.TestCase):
    """Tests for are_middle_names_in_conflict."""

    def test_middle_initial_matches_full_middle_name(self) -> None:
        """A lone middle initial matches the full name it abbreviates, with or
        without a trailing period."""
        for initial in ["L", "L."]:
            with self.subTest(initial=initial):
                self.assertFalse(
                    are_middle_names_in_conflict(
                        initial,
                        "LEE",
                        surnames_match_exactly=False,
                        dobs_match_exactly=False,
                    )
                )

    def test_differing_middle_initials_never_conflict(self) -> None:
        """Accepted cost: two bare initials are always within one edit of each
        other, so differing middle initials like L and M never conflict. A lone
        initial carries almost no signal. We only mark a conflict based on
        middle names in states that provide rich middle-name data, as opposed
        to states like ND that primarily provide middle initials only."""
        self.assertFalse(
            are_middle_names_in_conflict(
                "L",
                "M",
                surnames_match_exactly=False,
                dobs_match_exactly=False,
            )
        )

    def test_longer_shared_prefix_in_conflict(self) -> None:
        """Only a lone initial matches by prefix; a longer shared prefix does
        not, so genuinely different middle names still conflict (MAR vs
        MARIE)."""
        self.assertTrue(
            are_middle_names_in_conflict(
                "MAR",
                "MARIE",
                surnames_match_exactly=False,
                dobs_match_exactly=False,
            )
        )

    def test_different_middle_names_in_conflict(self) -> None:
        self.assertTrue(
            are_middle_names_in_conflict(
                "MARIE",
                "LYNN",
                surnames_match_exactly=False,
                dobs_match_exactly=False,
            )
        )


class TestAreNameSuffixesInConflict(unittest.TestCase):
    """Tests for are_name_suffixes_in_conflict."""

    def test_spelling_variants_of_one_suffix_not_in_conflict(self) -> None:
        self.assertFalse(are_name_suffixes_in_conflict("Junior", "JR"))
        self.assertFalse(are_name_suffixes_in_conflict("2nd", "II"))
        self.assertFalse(are_name_suffixes_in_conflict("5th", "V"))
        self.assertFalse(are_name_suffixes_in_conflict("Fifth", "5"))

    def test_equivalent_generation_markers_not_in_conflict(self) -> None:
        """Sources use JR and II (and SR and I) interchangeably for the same
        person, so they canonicalize together."""
        self.assertFalse(are_name_suffixes_in_conflict("Jr", "II"))
        self.assertFalse(are_name_suffixes_in_conflict("Senior", "I"))

    def test_different_canonical_suffixes_in_conflict(self) -> None:
        self.assertTrue(are_name_suffixes_in_conflict("Jr.", "Sr"))
        self.assertTrue(are_name_suffixes_in_conflict("II", "III"))

    def test_unrecognized_forms_compare_verbatim(self) -> None:
        """A suffix outside the canonical map (like a professional title) is
        compared as its stripped, uppercased self."""
        self.assertFalse(are_name_suffixes_in_conflict("MD", "M.D."))
        self.assertTrue(are_name_suffixes_in_conflict("MD", "Esq"))
        self.assertTrue(are_name_suffixes_in_conflict("MD", "JR"))

    def test_missing_suffix_not_in_conflict(self) -> None:
        self.assertFalse(are_name_suffixes_in_conflict(None, "JR"))


class TestAreDobsInConflict(unittest.TestCase):
    """Tests for are_dobs_in_conflict."""

    def test_equal_dates_not_in_conflict(self) -> None:
        self.assertFalse(
            are_dobs_in_conflict(
                datetime.date(1990, 1, 1),
                datetime.date(1990, 1, 1),
                surnames_and_given_names_match_exactly=False,
            )
        )

    def test_missing_dob_not_in_conflict(self) -> None:
        self.assertFalse(
            are_dobs_in_conflict(
                None,
                datetime.date(1990, 1, 1),
                surnames_and_given_names_match_exactly=False,
            )
        )
        self.assertFalse(
            are_dobs_in_conflict(
                datetime.date(1990, 1, 1),
                None,
                surnames_and_given_names_match_exactly=False,
            )
        )

    def test_typo_within_threshold_not_in_conflict(self) -> None:
        # Off-by-one day and an off-by-one year are both within the
        # edit-distance threshold and the year gap, so no corroboration is
        # needed.
        self.assertFalse(
            are_dobs_in_conflict(
                datetime.date(1990, 1, 1),
                datetime.date(1990, 1, 2),
                surnames_and_given_names_match_exactly=False,
            )
        )
        self.assertFalse(
            are_dobs_in_conflict(
                datetime.date(1990, 3, 7),
                datetime.date(1991, 3, 7),
                surnames_and_given_names_match_exactly=False,
            )
        )

    def test_month_day_swap_not_in_conflict(self) -> None:
        self.assertFalse(
            are_dobs_in_conflict(
                datetime.date(1994, 11, 2),
                datetime.date(1994, 2, 11),
                surnames_and_given_names_match_exactly=False,
            )
        )

    def test_edit_distance_at_threshold_not_in_conflict(self) -> None:
        # "1990-01-01" vs "1990-03-03": two substituted digits, not a swap, so
        # exactly at the threshold of 2 and still benign.
        self.assertFalse(
            are_dobs_in_conflict(
                datetime.date(1990, 1, 1),
                datetime.date(1990, 3, 3),
                surnames_and_given_names_match_exactly=False,
            )
        )

    def test_same_year_double_typo_not_in_conflict(self) -> None:
        """Accepted cost: two mistyped digits in the same year, like
        "1990-01-01" and "1990-11-11", are within the edit-distance threshold
        and the year gap, so they are forgiven with no corroboration."""
        self.assertFalse(
            are_dobs_in_conflict(
                datetime.date(1990, 1, 1),
                datetime.date(1990, 11, 11),
                surnames_and_given_names_match_exactly=False,
            )
        )

    def test_year_slip_requires_name_corroboration(self) -> None:
        """A difference within the edit-distance threshold whose years are
        more than one apart is a mistyped year digit that moves the birthdate
        a decade or a century. The names must vouch for it: with an exact
        surname and given-name match it is trusted as a typo, otherwise it
        conflicts."""
        year_slips = [
            (datetime.date(1946, 7, 19), datetime.date(1956, 7, 19)),  # decade
            (datetime.date(1990, 1, 1), datetime.date(1980, 1, 1)),  # decade
            (datetime.date(1901, 1, 1), datetime.date(2001, 1, 1)),  # century
        ]
        for dob_a, dob_b in year_slips:
            with self.subTest(dob_a=dob_a, dob_b=dob_b):
                self.assertTrue(
                    are_dobs_in_conflict(
                        dob_a, dob_b, surnames_and_given_names_match_exactly=False
                    )
                )
                self.assertFalse(
                    are_dobs_in_conflict(
                        dob_a, dob_b, surnames_and_given_names_match_exactly=True
                    )
                )

    def test_edit_distance_beyond_threshold_in_conflict(self) -> None:
        # "1990-01-01" vs "1993-03-03": three substituted digits, one past the
        # threshold of 2, so even exactly matching names cannot excuse it.
        self.assertTrue(
            are_dobs_in_conflict(
                datetime.date(1990, 1, 1),
                datetime.date(1993, 3, 3),
                surnames_and_given_names_match_exactly=True,
            )
        )

    def test_genuinely_different_dates_in_conflict(self) -> None:
        self.assertTrue(
            are_dobs_in_conflict(
                datetime.date(1990, 1, 1),
                datetime.date(1990, 6, 15),
                surnames_and_given_names_match_exactly=False,
            )
        )
        # Same year but not a swap (the days differ).
        self.assertTrue(
            are_dobs_in_conflict(
                datetime.date(1994, 11, 2),
                datetime.date(1994, 2, 12),
                surnames_and_given_names_match_exactly=False,
            )
        )


class TestSignalSymmetry(unittest.TestCase):
    """Every signal must be symmetric: swapping the two compared values cannot
    change whether they conflict. This holds today because Levenshtein distance
    and set operations are symmetric, but helpers like the lone-initial and
    token-subset checks make it easy to break in a refactor."""

    _NAME_PAIRS = [
        ("ANNE", "ANNE MARIE"),  # token subset
        ("L", "LEE"),  # lone initial
        ("BOB", "ROBERT"),  # nickname lookup
        ("JAIME", "JAMIE"),  # two-edit spelling drift
        ("MELISSA", "JONATHON"),  # genuinely different
        ("JR", "MICHAEL"),  # normalizes to empty
        (None, "MICHAEL"),  # missing
    ]

    def test_given_and_middle_name_signals_symmetric(self) -> None:
        for name_a, name_b in self._NAME_PAIRS:
            for corroborated in (False, True):
                for func in (are_given_names_in_conflict, are_middle_names_in_conflict):
                    with self.subTest(
                        func=func.__name__, a=name_a, b=name_b, gated=corroborated
                    ):
                        self.assertEqual(
                            func(
                                name_a,
                                name_b,
                                surnames_match_exactly=corroborated,
                                dobs_match_exactly=corroborated,
                            ),
                            func(
                                name_b,
                                name_a,
                                surnames_match_exactly=corroborated,
                                dobs_match_exactly=corroborated,
                            ),
                        )

    def test_surname_signal_symmetric(self) -> None:
        pairs = [
            ("LOPEZ", "LOPEZ SMITH"),
            ("GUSTAFSON", "GUSTAFSEN"),
            ("HALL", "MILLER"),
            (None, "MILLER"),
        ]
        for surname_a, surname_b in pairs:
            for hatch_open in (False, True):
                with self.subTest(a=surname_a, b=surname_b, hatch=hatch_open):
                    self.assertEqual(
                        are_surnames_in_conflict(
                            surname_a,
                            surname_b,
                            given_names_loosely_match=hatch_open,
                            dobs_match_exactly=hatch_open,
                        ),
                        are_surnames_in_conflict(
                            surname_b,
                            surname_a,
                            given_names_loosely_match=hatch_open,
                            dobs_match_exactly=hatch_open,
                        ),
                    )

    def test_suffix_signal_symmetric(self) -> None:
        for left, right in [("Jr", "Sr"), ("Junior", "II"), ("MD", "Esq")]:
            with self.subTest(a=left, b=right):
                self.assertEqual(
                    are_name_suffixes_in_conflict(left, right),
                    are_name_suffixes_in_conflict(right, left),
                )

    def test_dob_signal_symmetric(self) -> None:
        pairs = [
            (datetime.date(1994, 11, 2), datetime.date(1994, 2, 11)),  # swap
            (datetime.date(1990, 1, 1), datetime.date(1990, 1, 2)),  # typo
            (datetime.date(1980, 1, 1), datetime.date(1990, 1, 1)),  # year slip
            (datetime.date(1950, 6, 15), datetime.date(1988, 3, 4)),  # in conflict
        ]
        for dob_a, dob_b in pairs:
            for names_match in (False, True):
                with self.subTest(a=dob_a, b=dob_b, names_match=names_match):
                    self.assertEqual(
                        are_dobs_in_conflict(
                            dob_a,
                            dob_b,
                            surnames_and_given_names_match_exactly=names_match,
                        ),
                        are_dobs_in_conflict(
                            dob_b,
                            dob_a,
                            surnames_and_given_names_match_exactly=names_match,
                        ),
                    )


class TestNormalizeName(unittest.TestCase):
    """Tests for _normalize_name."""

    def test_uppercases_and_collapses_punctuation(self) -> None:
        self.assertEqual("LOPEZ SMITH", _normalize_name("Lopez-Smith"))

    def test_strips_parenthetical_alias(self) -> None:
        self.assertEqual("MARY KAY", _normalize_name("Mary Kay (Missy)"))

    def test_strips_embedded_suffix_token(self) -> None:
        self.assertEqual("JAMES", _normalize_name("James Jr"))
        # V is a recognized suffix token, like the other roman numerals.
        self.assertEqual("HENRY", _normalize_name("Henry V"))

    def test_suffix_only_name_is_kept_not_emptied(self) -> None:
        """A name that is nothing but a suffix token keeps it rather than
        normalizing away to empty and being read as a missing name (a person
        whose surname really is "JR")."""
        self.assertEqual("JR", _normalize_name("JR"))
        self.assertEqual("V", _normalize_name("V"))

    def test_collapses_apostrophe(self) -> None:
        self.assertEqual("O BRIEN", _normalize_name("O'Brien"))

    def test_removes_accents(self) -> None:
        self.assertEqual("JOSE", _normalize_name("José"))
        self.assertEqual("MUNOZ", _normalize_name("Muñoz"))
        self.assertEqual("RENEE", _normalize_name("Renée"))

    def test_accented_and_unaccented_spellings_normalize_alike(self) -> None:
        self.assertEqual(_normalize_name("José"), _normalize_name("Jose"))

    def test_letters_without_decomposition_are_stripped(self) -> None:
        """A letter with no NFKD decomposition (like the slashed O) has no base
        letter to reduce to and falls to the non-alphabetic strip."""
        self.assertEqual("S RENSEN", _normalize_name("Sørensen"))

    def test_none_normalizes_to_empty(self) -> None:
        self.assertEqual("", _normalize_name(None))
