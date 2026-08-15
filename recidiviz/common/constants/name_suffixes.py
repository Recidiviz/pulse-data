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
"""Shared vocabulary of personal-name generational suffixes (Jr, Sr, II,
...)."""

# Suffix tokens that count as an embedded generational suffix when they end a
# name field ("JAMES JR" -> "JAMES" + "JR"). This set deliberately covers fewer
# spellings than CANONICAL_NAME_SUFFIXES below because forms like "FIRST" and
# "SENIOR" are plausible surnames, and a bare "I" is too ambiguous to treat as
# a suffix. A unit test keeps this set inside the recognized suffix
# vocabulary of CANONICAL_NAME_SUFFIXES.
EMBEDDED_NAME_SUFFIX_TOKENS: tuple[str, ...] = ("JR", "SR", "II", "III", "IV", "V")

# Canonical forms of free-form name suffixes: every way of writing "the second
# holder of the name" (JR, Junior, II, 2nd) canonicalizes to II, and every way
# of writing "the elder" (SR, Senior, I) to I, since sources use them
# interchangeably for the same person.
CANONICAL_NAME_SUFFIXES: dict[str, str] = {
    "SR": "I",
    "SENIOR": "I",
    "1": "I",
    "1ST": "I",
    "FIRST": "I",
    "I": "I",
    "JR": "II",
    "JUNIOR": "II",
    "2": "II",
    "2ND": "II",
    "SECOND": "II",
    "II": "II",
    "3": "III",
    "3RD": "III",
    "THIRD": "III",
    "III": "III",
    "4": "IV",
    "4TH": "IV",
    "FOURTH": "IV",
    "IV": "IV",
    "5": "V",
    "5TH": "V",
    "FIFTH": "V",
    "V": "V",
}
