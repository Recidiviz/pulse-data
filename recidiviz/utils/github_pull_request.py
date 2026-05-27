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
"""Parsed representation of a GitHub pull request URL."""

import re

import attr


@attr.s(frozen=True)
class GithubPullRequest:
    """A parsed GitHub pull request URL, e.g.
    'https://github.com/Recidiviz/pulse-data/pull/123'."""

    _PULL_REQUEST_URL_REGEX = re.compile(
        r"https://github\.com/(?P<owner>[^/]+)/(?P<repo>[^/]+)/pull/(?P<number>\d+)"
    )

    owner: str = attr.ib()
    repo: str = attr.ib()
    number: int = attr.ib()

    @property
    def url(self) -> str:
        return f"https://github.com/{self.owner}/{self.repo}/pull/{self.number}"

    @classmethod
    def from_url(cls, url: str) -> "GithubPullRequest":
        """Parses a GitHub pull request URL. Raises ValueError if the URL
        doesn't match the expected format."""
        match = cls._PULL_REQUEST_URL_REGEX.fullmatch(url)
        if not match:
            raise ValueError(f"Invalid PR URL: {url}")
        return cls(
            owner=match.group("owner"),
            repo=match.group("repo"),
            number=int(match.group("number")),
        )
