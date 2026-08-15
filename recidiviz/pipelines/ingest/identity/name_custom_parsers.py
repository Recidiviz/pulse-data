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
"""Custom parsers for identity mappings that make name values conform to what
the IdentityName and IdentityAlias validators accept.

IdentityName and IdentityAlias validate their name parts strictly, so a stray
character left behind by a source would crash entity construction. Each parser
here deletes the characters the corresponding validator does not permit,
normalizes whitespace, and then runs the entity's own validator, returning None
when nothing usable is left.

State-specific unwrapping of name columns (embedded aliases, note markers,
generational suffixes glued onto a name part) belongs in that state's identity
views, not here. This module handles only the state-agnostic question of
whether a value is a name the entity will accept.
"""
import re
import unicodedata

from recidiviz.common.attr_validator_checks import passes_validator
from recidiviz.common.attr_validators import (
    NAME_PART_CHAR_PATTERN,
    NAME_SUFFIX_CHAR_PATTERN,
    AttrValidator,
    is_valid_name_part,
    is_valid_name_suffix,
)

_WHITESPACE_RUN_PATTERN = re.compile(r"\s+")


def clean_name_part_or_null(name: str) -> str | None:
    """Returns |name| with characters a name part may not contain removed and
    whitespace normalized ("JOHN*  PAUL" -> "JOHN PAUL"), or None if nothing
    that passes the name-part validator is left ("*" -> None)."""
    return _clean_or_null(name, NAME_PART_CHAR_PATTERN, is_valid_name_part)


def clean_name_suffix_or_null(suffix: str) -> str | None:
    """Returns |suffix| with characters a name suffix may not contain removed
    and whitespace normalized ("(3RD)" -> "3RD"), or None if nothing that
    passes the name-suffix validator is left. Digits survive, since "3RD" and
    "2" are real suffixes; an over-long result nulls out."""
    return _clean_or_null(suffix, NAME_SUFFIX_CHAR_PATTERN, is_valid_name_suffix)


def _clean_or_null(
    value: str, allowed_char_pattern: re.Pattern[str], validator: AttrValidator
) -> str | None:
    """Returns |value| reduced to the characters matching |allowed_char_pattern|
    with whitespace runs collapsed and the result trimmed, or None when that
    leaves an empty string or a value |validator| still rejects. NFC-normalizes
    first so a decomposed accent (a base letter plus a combining mark, which the
    letter patterns do not match) becomes its precomposed letter rather than
    losing the mark."""
    if not value:
        return None
    value = unicodedata.normalize("NFC", value)
    permitted_chars_only = "".join(
        char for char in value if allowed_char_pattern.fullmatch(char)
    )
    cleaned = _WHITESPACE_RUN_PATTERN.sub(" ", permitted_chars_only).strip()
    if not cleaned or not passes_validator(validator, cleaned):
        return None
    return cleaned
