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
"""GithubIssue data class for referencing GitHub issues."""

import re
from typing import Match

import attr

from recidiviz.issue_tracking.issue import Issue


@attr.s(frozen=True, kw_only=True)
class GithubIssue(Issue):
    """A reference to a GitHub issue, e.g. '#123' or 'Recidiviz/pulse-data#123'."""

    repo: str = attr.ib()
    number: int = attr.ib()

    @classmethod
    def issue_regex(cls) -> str:
        return r"(?P<repo>\S+\/\S+)?#(?P<issue>[0-9]+)"

    @classmethod
    def _from_match(cls, match: Match[str], *, default_repo: str) -> "GithubIssue":
        return cls(
            repo=match.group("repo") or default_repo,
            number=int(match.group("issue")),
        )

    @classmethod
    def from_string(cls, issue_string: str, *, default_repo: str) -> "GithubIssue":
        match = re.fullmatch(cls.issue_regex(), issue_string)
        if match is None:
            raise ValueError(
                "String did not match format '<owner>/<repo>#<issue>' (e.g. 'Recidiviz/pulse-data#123')"
            )
        return cls._from_match(match, default_repo=default_repo)

    _ISSUE_URL_REGEX = re.compile(
        r"https://github\.com/(?P<repo>[^/]+/[^/]+)/issues/(?P<number>\d+)"
    )

    @property
    def url(self) -> str:
        return f"https://github.com/{self.repo}/issues/{self.number}"

    @classmethod
    def from_url(cls, url: str) -> "GithubIssue":
        """Parses a GitHub issue URL like
        'https://github.com/Recidiviz/pulse-data/issues/123'."""
        match = cls._ISSUE_URL_REGEX.fullmatch(url)
        if match is None:
            raise ValueError(f"String did not match a GitHub issue URL: {url}")
        return cls(
            repo=match.group("repo"),
            number=int(match.group("number")),
        )

    def __str__(self) -> str:
        return f"{self.repo}#{self.number}"
