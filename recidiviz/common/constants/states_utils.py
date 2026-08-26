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
#   (?P<state>us_[a-z]{2,}) - Captures "us_" followed by 2 or more lowercase
#                           letters into a group named "state". Two letters covers
#                           real state codes ("us_ca"); the open-ended {2,} also
#                           admits longer non-state tenant codes ("us_nyc"). The
#                           token so captured is then filtered by enum membership
#                           below, so non-code tokens like "us_states" — which now
#                           match the shape — are dropped rather than returned.
#   (?=_|$)               - Lookahead asserting "_" or end-of-string follows,
#                           without consuming it. This is the boundary that keeps
#                           each token whole: "us_nyc" is captured as "us_nyc"
#                           (not "us_ny"), and "us_st" inside "us_states" is not a
#                           partial match.
_STATE_CODE_PATTERN = re.compile(r"(?:^|(?<=_))(?P<state>us_[a-z]{2,})(?=_|$)")


def find_state_codes_in_str(s: str) -> set[StateCode]:
    """Returns all valid StateCode values found in the underscore-delimited string."""
    found: set[StateCode] = set()
    for match in _STATE_CODE_PATTERN.finditer(s.lower()):
        try:
            found.add(StateCode(match.group("state").upper()))
        except ValueError:
            pass
    return found
