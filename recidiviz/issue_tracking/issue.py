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
"""Abstract base class for issues/tasks in external task management systems,
plus the UrlIssue concrete subclass for URL-based issue references.
"""

import abc
import re

import attr

_TODO_WITH_ARGS_REGEX = re.compile(r"TODO\((?P<issue_reference>[^)]+)\)")


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
        the appropriate Issue subclass. Only subclasses whose modules have
        already been imported will be considered."""
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


@attr.s(frozen=True, kw_only=True)
class UrlIssue(Issue):
    url: str = attr.ib()

    @classmethod
    def issue_regex(cls) -> str:
        return r"(?P<issue_url>http\S+)"

    @classmethod
    def _from_match(cls, match: re.Match[str]) -> "UrlIssue":
        return cls(url=match.group("issue_url"))

    @classmethod
    def from_string(cls, issue_string: str) -> "UrlIssue":
        match = re.fullmatch(cls.issue_regex(), issue_string)
        if match is None:
            raise ValueError("String did not match a URL starting with 'http'")
        return cls._from_match(match)

    def __str__(self) -> str:
        return self.url
