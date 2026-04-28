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
"""Utilities for dealing with StateCodes."""
import re

from recidiviz.common.constants.states import StateCode

# Finds "us_xx" state code tokens inside underscore-delimited strings like
# dataset or table names. Breakdown of the pattern:
#
#   (?:^|(?<=_))          - Position must be at start-of-string OR right after "_".
#                           (?:...) is a non-capturing group (groups alternatives
#                           without creating a capture). (?<=_) is a lookbehind
#                           that asserts "_" precedes this position without
#                           consuming it.
#   (?P<state>us_[a-z]{2}) - Captures "us_" followed by exactly 2 lowercase
#                           letters into a group named "state". The {2} is what
#                           prevents "us_states" from matching — "st" is only 2
#                           chars, but the next char is "a", not "_" or end-of-
#                           string, so the lookahead below rejects it.
#   (?=_|$)               - Lookahead asserting "_" or end-of-string follows,
#                           without consuming it. This is the boundary that
#                           rejects partial matches like "us_st" inside
#                           "us_states".
_STATE_CODE_PATTERN = re.compile(r"(?:^|(?<=_))(?P<state>us_[a-z]{2})(?=_|$)")


def find_state_codes_in_str(s: str) -> set[StateCode]:
    """Returns all valid StateCode values found in the underscore-delimited string."""
    found: set[StateCode] = set()
    for match in _STATE_CODE_PATTERN.finditer(s.lower()):
        try:
            found.add(StateCode(match.group("state").upper()))
        except ValueError:
            pass
    return found
