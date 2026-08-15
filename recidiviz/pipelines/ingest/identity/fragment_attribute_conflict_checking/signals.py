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
"""Conflict signals for the identity pipeline's intra-cluster checks.

Pure functions that decide whether two fragments' divergent values for an
attribute are genuinely in conflict (they cannot belong to one person), or
differ benignly due to a spelling or data-entry error, a name change or the
use of a nickname.

Nulls never conflict: every rule returns False when either side is missing, so
external-id-only fragments and sparse sources pass trivially.
"""
import datetime
import re
import unicodedata

import Levenshtein

from recidiviz.common.constants.name_suffixes import (
    CANONICAL_NAME_SUFFIXES,
    EMBEDDED_NAME_SUFFIX_TOKENS,
)
from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking.nickname_equivalence import (
    are_names_nickname_equivalent,
)

_PARENTHETICAL_PATTERN = re.compile(r"\([^)]*\)")
_EMBEDDED_SUFFIX_PATTERN = re.compile(
    rf"\b(?:{'|'.join(EMBEDDED_NAME_SUFFIX_TOKENS)})\b"
)
_NON_ALPHA_PATTERN = re.compile(r"[^A-Z ]")
_WHITESPACE_PATTERN = re.compile(r"\s+")

# Matches "ST" as a whole token; normalize_name expands it to "SAINT" so
# "ST CLAIRE" and "SAINT CLAIRE" compare equal.
_SAINT_ABBREVIATION_PATTERN = re.compile(r"\bST\b")

# Levenshtein distance on the normalized surname above which surnames
# genuinely conflict.
_SURNAME_LEVENSHTEIN_THRESHOLD = 2

# Levenshtein distance within which two short surnames are considered the same.
# (Long surnames that differ by two edits are likely the same names, but short
# surnames that differ by this much are usually different names (MILLER and
# MILLS, JONES and JAMES)
_SHORT_SURNAME_LEVENSHTEIN_THRESHOLD = 1
_SHORT_SURNAME_MAX_LENGTH = 5

# Levenshtein distance within which two given or middle names count as spelling
# drift of the same name when surname and date of birth corroborate (JAIME vs
# JAMIE).
_CORROBORATED_NAME_LEVENSHTEIN_THRESHOLD = 2

# Levenshtein distance on the ISO YYYY-MM-DD strings within which two dates of
# birth count as a data-entry error.
_DOB_LEVENSHTEIN_THRESHOLD = 2

# Year gap within which a DOB difference inside the edit-distance threshold is
# trusted as a typo with no other evidence. A bigger gap within the threshold
# means a mistyped year digit that moves the birthdate a decade or a century,
# so it is excused only when the names vouch for the pair.
_DOB_UNGATED_YEAR_GAP = 1


def names_match_exactly(name_a: str | None, name_b: str | None) -> bool:
    """Returns whether two names are identical after normalization. Used for
    corroboration. A missing name matches nothing, including another missing
    name.
    """
    normalized_a, normalized_b = normalize_name(name_a), normalize_name(name_b)
    if not normalized_a or not normalized_b:
        return False
    return normalized_a == normalized_b


def names_match_loosely(name_a: str | None, name_b: str | None) -> bool:
    """Returns whether two names match loosely after normalization, that is,
    within one Levenshtein edit, or one's tokens a subset of the other's. Used
    for corroboration. A missing name matches nothing, including another missing
    name.
    """
    normalized_a, normalized_b = normalize_name(name_a), normalize_name(name_b)
    if not normalized_a or not normalized_b:
        return False
    if normalized_a == normalized_b:
        return True
    if Levenshtein.distance(normalized_a, normalized_b) <= 1:
        return True
    return _is_token_subset(normalized_a, normalized_b)


def are_surnames_in_conflict(
    surname_a: str | None,
    surname_b: str | None,
    *,
    given_names_match_loosely: bool,
    dobs_match_exactly: bool,
) -> bool:
    """Returns whether two surnames are too different to belong to the same
    person, that is, they:

    - differ by more than the Levenshtein threshold, not mere spelling drift
      like "GUSTAFSON" and "GUSTAFSEN"; a surname of five letters or fewer
      gets a one-edit budget instead of two, so "MILLER" and "MILLS" conflict,
    - are not a token-subset of one another, like "LOPEZ" and "LOPEZ SMITH"
    - are not excused by the name-change hatch, where the given name loosely
      matches and the date of birth matches exactly.
    """
    normalized_a, normalized_b = normalize_name(surname_a), normalize_name(surname_b)
    if not normalized_a or not normalized_b:
        return False
    threshold = (
        _SHORT_SURNAME_LEVENSHTEIN_THRESHOLD
        if min(len(normalized_a), len(normalized_b)) <= _SHORT_SURNAME_MAX_LENGTH
        else _SURNAME_LEVENSHTEIN_THRESHOLD
    )
    if Levenshtein.distance(normalized_a, normalized_b) <= threshold:
        return False
    if _is_token_subset(normalized_a, normalized_b):
        return False
    if given_names_match_loosely and dobs_match_exactly:
        # The divergence is likely due to a name change.
        return False
    return True


def are_given_names_in_conflict(
    given_name_a: str | None,
    given_name_b: str | None,
    *,
    surnames_match_exactly: bool,
    dobs_match_exactly: bool,
) -> bool:
    """Returns whether two given names are too different to belong to the same
    person, that is, they:

    - differ by more than one character, not mere spelling drift like "JON"
      and "JOHN",
    - are not a token-subset of one another, like "ANNE" and "ANNE MARIE", and
    - are not excused, when the surname and date of birth match exactly, by
      being nickname-equivalent like "BOB" and "ROBERT" or within two
      characters like "JAIME" and "JAMIE".
    """
    return _are_given_or_middle_names_in_conflict(
        given_name_a,
        given_name_b,
        surnames_match_exactly=surnames_match_exactly,
        dobs_match_exactly=dobs_match_exactly,
        allow_initial_match=False,
    )


def are_middle_names_in_conflict(
    middle_name_a: str | None,
    middle_name_b: str | None,
    *,
    surnames_match_exactly: bool,
    dobs_match_exactly: bool,
) -> bool:
    """Returns whether two middle names are too different to belong to the
    same person. The rule matches are_given_names_in_conflict, plus one leniency: a
    lone middle initial matches the full name it abbreviates. So they:

    - differ by more than one character, not mere spelling drift like "JON"
      and "JOHN",
    - are not a token-subset of one another, like "ANNE" and "ANNE MARIE",
    - are not a lone initial of the other, like "L" (or "L.") and "LEE", and
    - are not excused, when the surname and date of birth match exactly, by
      being nickname-equivalent like "BOB" and "ROBERT" or within two
      characters like "JAIME" and "JAMIE".
    """
    return _are_given_or_middle_names_in_conflict(
        middle_name_a,
        middle_name_b,
        surnames_match_exactly=surnames_match_exactly,
        dobs_match_exactly=dobs_match_exactly,
        allow_initial_match=True,
    )


def are_name_suffixes_in_conflict(suffix_a: str | None, suffix_b: str | None) -> bool:
    """Returns whether two name suffixes are too different to belong to the
    same person. Equivalent free-form spellings are canonicalized first, then:

    - interchangeable forms agree and never conflict, like "Junior", "Jr" and
      "II" (all the second) or "Senior" and "I" (the elder),
    - genuinely different generations conflict, like "JR" and "SR" or "II" and
      "III", which usually mean two people sharing IDs.
    """
    canonical_a, canonical_b = canonical_suffix(suffix_a), canonical_suffix(suffix_b)
    if not canonical_a or not canonical_b:
        return False
    return canonical_a != canonical_b


def are_dobs_in_conflict(
    dob_a: datetime.date | None,
    dob_b: datetime.date | None,
    *,
    surnames_and_given_names_match_exactly: bool,
) -> bool:
    """Returns whether two dates of birth are too different to belong to the
    same person, that is, they:

    - are not a month/day swap in the same year, the MM-DD / DD-MM format
      confusion like "1994-11-02" and "1994-02-11",
    - differ by more than a small edit distance on the ISO YYYY-MM-DD strings,
      not a recognized data-entry typo like a wrong digit, an adjacent-digit
      transposition or an off-by-one day, and
    - are not excused by the year-slip escape: a difference within the edit
      distance but with years more than one apart is a single mistyped year
      digit that moves the birthdate a decade or a century ("1990-01-01" and
      "1980-01-01"), so it is trusted only when the surname and given name
      both match exactly.
    """
    if dob_a is None or dob_b is None:
        return False
    if dob_a == dob_b:
        return False
    if _is_month_day_swap(dob_a, dob_b):
        return False
    if (
        Levenshtein.distance(dob_a.isoformat(), dob_b.isoformat())
        <= _DOB_LEVENSHTEIN_THRESHOLD
    ):
        if abs(dob_a.year - dob_b.year) <= _DOB_UNGATED_YEAR_GAP:
            return False
        if surnames_and_given_names_match_exactly:
            return False
    return True


def normalize_name(raw_name: str | None) -> str:
    """Returns the normalized form used by every name comparison. Specifically
    it:

    - uppercases the name;
    - replaces accented letters with their base letters (JOSE for JOSE with an
      accented E);
    - removes parenthetical aliases and embedded suffix tokens;
    - collapses punctuation (hyphens included) to single spaces;
    - canonicalizes the whole token "ST" to "SAINT" (so "ST CLAIRE" and
      "SAINT CLAIRE" compare equal).

    Returns the empty string for a missing name. An embedded suffix is not
    removed when it is the whole name, so a surname of just "JR" does not
    normalize away to empty.
    """
    if raw_name is None:
        return ""
    name = _remove_accents(raw_name.upper())
    name = _PARENTHETICAL_PATTERN.sub(" ", name)
    without_suffix = _strip_non_alpha(_EMBEDDED_SUFFIX_PATTERN.sub(" ", name))
    result = without_suffix or _strip_non_alpha(name)
    return _SAINT_ABBREVIATION_PATTERN.sub("SAINT", result)


def canonical_suffix(raw_suffix: str | None) -> str:
    """Returns the canonical form of a free-form suffix, the stripped
    uppercased value itself for unrecognized forms, or the empty string for a
    missing suffix."""
    if raw_suffix is None:
        return ""
    stripped = re.sub(r"[^A-Z0-9]", "", raw_suffix.upper())
    # .get rather than []: unrecognized forms are expected in free-form data
    # and simply compare as themselves.
    return CANONICAL_NAME_SUFFIXES.get(stripped, stripped)


def _is_token_subset(normalized_a: str, normalized_b: str) -> bool:
    """Returns whether one normalized name's tokens are a subset of the
    other's (ANNE MARIE vs ANNE; LOPEZ SMITH vs LOPEZ)."""
    tokens_a, tokens_b = set(normalized_a.split()), set(normalized_b.split())
    if not tokens_a or not tokens_b:
        return False
    return tokens_a <= tokens_b or tokens_b <= tokens_a


def _are_given_or_middle_names_in_conflict(
    name_a: str | None,
    name_b: str | None,
    *,
    surnames_match_exactly: bool,
    dobs_match_exactly: bool,
    allow_initial_match: bool,
) -> bool:
    """Shared implementation of the given-name and middle-name rules, which
    differ only in whether a lone initial matches the full name: middle names
    accept it (a stored middle initial abbreviates the full middle name),
    given names do not."""
    normalized_a, normalized_b = normalize_name(name_a), normalize_name(name_b)
    if not normalized_a or not normalized_b:
        return False
    if (
        normalized_a == normalized_b
        or Levenshtein.distance(normalized_a, normalized_b) <= 1
    ):
        return False
    if _is_token_subset(normalized_a, normalized_b):
        return False
    if allow_initial_match and _is_initial_match(normalized_a, normalized_b):
        return False

    # Same person, different form of the name: trusted only when other
    # identifying fields corroborate, because a nickname pair shares few
    # letters and two edits can reach a genuinely different name.
    if surnames_match_exactly and dobs_match_exactly:
        if are_names_nickname_equivalent(normalized_a, normalized_b):
            return False
        if (
            Levenshtein.distance(normalized_a, normalized_b)
            <= _CORROBORATED_NAME_LEVENSHTEIN_THRESHOLD
        ):
            return False

    return True


def _is_month_day_swap(dob_a: datetime.date, dob_b: datetime.date) -> bool:
    """Returns whether the two dates share a year with month and day
    exchanged. Only dates whose month and day are both at most 12 can swap,
    so this cannot misfire on unambiguous dates."""
    return (
        dob_a.year == dob_b.year
        and dob_a.month == dob_b.day
        and dob_a.day == dob_b.month
        and dob_a.month != dob_a.day
    )


def _remove_accents(name: str) -> str:
    """Returns the name with accented letters replaced with their base letters,
    so the same name spelled with and without accents compares as equal. Must
    run before the non-alphabetic strip, which would otherwise turn each
    accented letter into a space and split the name into separate tokens
    (MUNOZ with an accented N would become MU OZ). Letters with no
    decomposition (like the slashed O) have no base letter and still fall to
    the non-alphabetic strip."""
    decomposed = unicodedata.normalize("NFKD", name)
    return "".join(c for c in decomposed if not unicodedata.combining(c))


def _strip_non_alpha(name: str) -> str:
    """Returns the name with every non-letter collapsed to single spaces and
    the result trimmed."""
    return _WHITESPACE_PATTERN.sub(" ", _NON_ALPHA_PATTERN.sub(" ", name)).strip()


def _is_initial_match(normalized_a: str, normalized_b: str) -> bool:
    """Returns whether one normalized name is a lone initial matching the
    other's first letter (L or L. vs LEE). normalize_name strips the period,
    so a bare initial and one written with a trailing period compare alike."""
    if not normalized_a or not normalized_b:
        return False
    if len(normalized_a) == 1:
        return normalized_b.startswith(normalized_a)
    if len(normalized_b) == 1:
        return normalized_a.startswith(normalized_b)
    return False
