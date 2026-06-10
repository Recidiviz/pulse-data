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
"""LinearIssue data class for referencing Linear issues."""

import re
from typing import Match

import attr

from recidiviz.issue_tracking.issue import Issue
from recidiviz.issue_tracking.linear.linear_types import LinearTeamKey

LINEAR_ISSUE_URL_BASE = "https://linear.app/recidiviz/issue/"


@attr.s(frozen=True, kw_only=True)
class LinearIssue(Issue):
    """A reference to a Linear issue, e.g. 'OBT-12345'."""

    team_prefix: LinearTeamKey = attr.ib()
    number: int = attr.ib()

    @property
    def issue_identifier(self) -> str:
        return f"{self.team_prefix}-{self.number}"

    @property
    def url(self) -> str:
        return f"{LINEAR_ISSUE_URL_BASE}{self.issue_identifier}"

    @classmethod
    def issue_regex(cls) -> str:
        return r"(?P<team_prefix>[A-Z]+)-(?P<linear_number>\d+)"

    @classmethod
    def _from_match(cls, match: Match[str]) -> "LinearIssue":
        team_prefix = match.group("team_prefix")
        number = int(match.group("linear_number"))
        return cls(
            team_prefix=team_prefix,
            number=number,
        )

    @classmethod
    def from_string(cls, issue_string: str) -> "LinearIssue":
        match = re.fullmatch(cls.issue_regex(), issue_string)
        if match is None:
            raise ValueError(
                "String did not match format '<PREFIX>-<NUMBER>' (e.g. 'OBT-12345')"
            )
        return cls._from_match(match)

    def __str__(self) -> str:
        return self.issue_identifier
