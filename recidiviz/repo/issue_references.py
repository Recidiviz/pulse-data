# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Utilities for parsing TODOs in our codebase."""

import re
import subprocess
from typing import DefaultDict, Dict, List, Match

import attr

DEFAULT_REPO = "Recidiviz/pulse-data"


@attr.s(frozen=True, kw_only=True)
class GithubIssue:
    # The repository for the issue as '<owner>/<repo>' (e.g. 'Recidiviz/pulse-data')
    repo: str = attr.ib()

    # The issue number (e.g. 123)
    number: int = attr.ib()

    ISSUE_REGEX = r"(?P<repo>\S+\/\S+)?#(?P<issue>[0-9]+)"

    @classmethod
    def from_match(cls, match: Match[str]) -> "GithubIssue":
        return cls(
            repo=match.group("repo") or DEFAULT_REPO,
            number=int(match.group("issue")),
        )

    @classmethod
    def from_string(cls, issue_string: str) -> "GithubIssue":
        match = re.search(cls.ISSUE_REGEX, issue_string)
        if match is None:
            raise ValueError(
                "String did not match format '<owner>/<repo>#<issue>' (e.g. 'Recidiviz/pulse-data#123')"
            )
        return cls.from_match(match)

    def __str__(self) -> str:
        return f"{self.repo}#{self.number}"


@attr.s(frozen=True, kw_only=True)
class CodeReference:
    # The filepath relative to the root of the repo (e.g. 'recidiviz/common/date.py')
    filepath: str = attr.ib()

    # The line that the reference is on (e.g. 23)
    line_number: int = attr.ib()

    line_text: str = attr.ib()

    def __str__(self) -> str:
        return f"{self.filepath}:{self.line_number}"


TODO_REGEX = rf"TODO\({GithubIssue.ISSUE_REGEX}\)"


def _read_todo_lines_from_codebase() -> List[str]:
    # Gets all TODOs:
    # - Gets all files that are tracked by our git repo (so as to skip those in .gitignore)
    # - Runs grep over each file, pulling lines that contain "TODO"
    res = subprocess.run(
        "git ls-files -z | xargs -0 grep -n -e 'TODO'",
        shell=True,
        stdout=subprocess.PIPE,
        check=True,
    )
    return res.stdout.decode().splitlines()


def get_issue_references() -> Dict[GithubIssue, List[CodeReference]]:
    todo_regex = re.compile(TODO_REGEX)
    issue_references: Dict[GithubIssue, List[CodeReference]] = DefaultDict(list)

    for todo_line in _read_todo_lines_from_codebase():
        filepath, lineno, line_text = todo_line.split(":", 2)
        match = re.search(todo_regex, line_text)

        # This will be None for non-Github issue references that are allowed by our
        # lint rule, e.g. TODO(https://issuetracker.google.com/issues/000)
        if match is not None:
            issue_references[GithubIssue.from_match(match)].append(
                CodeReference(
                    line_text=line_text,
                    filepath=filepath,
                    line_number=int(lineno),
                )
            )
    return issue_references


def to_markdown(issue_references: Dict[GithubIssue, List[CodeReference]]) -> str:
    lines = []
    for issue in sorted(issue_references.keys()):
        lines.append(f"* {issue}")
        for reference in sorted(issue_references[issue]):
            lines.append(f"  * {reference}")
    return "\n".join(lines)
