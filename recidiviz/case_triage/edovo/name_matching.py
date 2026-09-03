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
"""Rules for comparing an Edovo-submitted learner name to the name we store.

Per the API spec, ``first_name`` and ``last_name`` verify the
``person_external_id`` match; they never resolve a person on their own. So these
rules answer one question: given a person the submitted id already resolved to,
is the submitted name the same name we hold for them?

The comparison is deliberately forgiving about how a name is written and strict
about which name it is. Both sides are reduced to their ASCII letters, so
punctuation, spacing, accents, and case never cause a mismatch — 'O'Brien',
'OBrien', and 'Obrien' all compare equal. What survives that reduction must
match exactly, so a genuinely different name is always caught.

Each rule answers "does this disagree with what we hold?", so a stored name we
do not have reads as agreement rather than as a mismatch. An incomplete record
on our side is not identifier drift on Edovo's, and it must not be reported as
though it were.
"""
import re
import unicodedata

_NON_LETTER_REGEX = re.compile(r"[^A-Z]")


def normalized_name(name: str) -> str:
    """Returns |name| reduced to its comparable core: uppercase ASCII letters.

    Accented characters decompose to their ASCII base ('Ñ' -> 'N'), and every
    character that is not a letter — spaces, hyphens, apostrophes, periods — is
    dropped. Returns an empty string for a name with no letters at all.
    """
    ascii_name = (
        unicodedata.normalize("NFKD", name)
        .encode("ascii", errors="ignore")
        .decode("ascii")
    )
    return _NON_LETTER_REGEX.sub("", ascii_name.upper())


def _first_token(name: str) -> str:
    """Returns the normalized form of |name|'s first whitespace-separated token."""
    tokens = name.split()
    return normalized_name(tokens[0]) if tokens else ""


def given_names_match(*, submitted_first_name: str, stored_given_names: str) -> bool:
    """Returns True if |submitted_first_name| does not disagree with |stored_given_names|.

    Our ``given_names`` may hold a middle name that Edovo does not send (we
    store 'MARY JANE', Edovo sends 'MARY'), or the reverse. So a match on either
    the whole normalized value or the first token alone counts, which tolerates
    a middle name on either side without accepting a different first name.

    A stored value holding no usable name cannot disagree with anything, so it
    counts as agreeing — an incomplete record on our side is not identifier
    drift on Edovo's.
    """
    if not normalized_name(stored_given_names):
        return True
    if normalized_name(submitted_first_name) == normalized_name(stored_given_names):
        return True
    # A token of no letters normalizes away, and two such names would then
    # compare equal, so a real token is required on both sides.
    submitted_token = _first_token(submitted_first_name)
    return bool(submitted_token) and submitted_token == _first_token(stored_given_names)


def surnames_match(*, submitted_last_name: str, stored_surname: str) -> bool:
    """Returns True if |submitted_last_name| does not disagree with |stored_surname|.

    Compared whole rather than by token: normalization already removes the
    spaces and hyphens that make a compound surname ('Van Der Berg',
    'van-der-berg') look different, so the full value can be compared without
    losing the distinction between two similar surnames.

    A stored surname holding no usable name counts as agreeing, for the reason
    given on ``given_names_match``.
    """
    if not normalized_name(stored_surname):
        return True
    return normalized_name(submitted_last_name) == normalized_name(stored_surname)
