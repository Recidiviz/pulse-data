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
"""Classes representing issues/tasks in external task management systems
(GitHub, Linear, or URL-based), with convenience helpers for constructing
these issues from identifier strings.
"""

import abc
import re
from typing import Match

import attr

DEFAULT_REPO = "Recidiviz/pulse-data"


@attr.s(frozen=True, kw_only=True)
class Issue(abc.ABC):
    """Base class for references to issues in external trackers (GitHub, Linear,
    or a URL-based tracker). Each subclass defines a regex for its identifier
    format and a factory to parse it from a string."""

    @classmethod
    @abc.abstractmethod
    def issue_regex(cls) -> str:
        """Returns a regex pattern that matches this issue type's identifier
        (e.g. '#123', 'OBT-456', 'https://...')."""

    @classmethod
    @abc.abstractmethod
    def from_string(cls, issue_string: str) -> "Issue":
        """Parses a bare issue identifier string into this type. Raises
        ValueError if the string doesn't match."""

    @classmethod
    def todo_regex(cls) -> str:
        """Returns a regex pattern matching a TODO referencing this issue type,
        e.g. 'TODO(#123)' or 'TODO(OBT-456)'."""
        return rf"TODO\({cls.issue_regex()}\)"

    @classmethod
    def from_todo(cls, todo_string: str) -> "Issue":
        """Parses a TODO string like 'TODO(#12345)' or 'TODO(OBT-789)' into
        the appropriate Issue subclass."""
        match = re.fullmatch(_TODO_WITH_ARGS_REGEX, todo_string)
        if match is None:
            raise ValueError(f"Unrecognized TODO format: {todo_string}")
        issue_ref_string = match.group("issue_reference")
        for subclass in cls.__subclasses__():
            try:
                return subclass.from_string(issue_ref_string)
            except ValueError:
                continue
        raise ValueError(f"Unrecognized TODO format: {todo_string}")


_TODO_WITH_ARGS_REGEX = re.compile(r"TODO\((?P<issue_reference>[^)]+)\)")


@attr.s(frozen=True, kw_only=True)
class GithubIssue(Issue):
    """A reference to a GitHub issue, e.g. '#123' or 'Recidiviz/pulse-data#123'."""

    repo: str = attr.ib()
    number: int = attr.ib()

    @classmethod
    def issue_regex(cls) -> str:
        return r"(?P<repo>\S+\/\S+)?#(?P<issue>[0-9]+)"

    @classmethod
    def _from_match(cls, match: Match[str]) -> "GithubIssue":
        return cls(
            repo=match.group("repo") or DEFAULT_REPO,
            number=int(match.group("issue")),
        )

    @classmethod
    def from_string(cls, issue_string: str) -> "GithubIssue":
        match = re.fullmatch(cls.issue_regex(), issue_string)
        if match is None:
            raise ValueError(
                "String did not match format '<owner>/<repo>#<issue>' (e.g. 'Recidiviz/pulse-data#123')"
            )
        return cls._from_match(match)

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


@attr.s(frozen=True, kw_only=True)
class LinearIssue(Issue):
    """A reference to a Linear issue, e.g. 'OBT-12345'."""

    team_prefix: str = attr.ib()
    number: int = attr.ib()

    @property
    def issue_identifier(self) -> str:
        return f"{self.team_prefix}-{self.number}"

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


@attr.s(frozen=True, kw_only=True)
class UrlIssue(Issue):
    url: str = attr.ib()

    @classmethod
    def issue_regex(cls) -> str:
        return r"(?P<issue_url>http\S+)"

    @classmethod
    def _from_match(cls, match: Match[str]) -> "UrlIssue":
        return cls(url=match.group("issue_url"))

    @classmethod
    def from_string(cls, issue_string: str) -> "UrlIssue":
        match = re.fullmatch(cls.issue_regex(), issue_string)
        if match is None:
            raise ValueError("String did not match a URL starting with 'http'")
        return cls._from_match(match)

    def __str__(self) -> str:
        return self.url
