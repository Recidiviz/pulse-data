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
"""Utilities for finding and displaying issue references (TODOs) in our
codebase."""

import re
import subprocess
from typing import DefaultDict, Dict, List, Mapping

import attr

from recidiviz.repo.issue import (
    _TODO_WITH_ARGS_REGEX,
    DEFAULT_REPO,
    GithubIssue,
    Issue,
    LinearIssue,
)


@attr.s(frozen=True, kw_only=True)
class CodeReference:
    filepath: str = attr.ib()
    line_number: int = attr.ib()
    line_text: str = attr.ib()

    def __str__(self) -> str:
        return f"{self.filepath}:{self.line_number}"


_COMMIT_SHA_REGEX = re.compile(r"^[0-9a-f]{4,40}$")


def _find_todo_code_references(commit_ref: str) -> List[CodeReference]:
    """Returns a CodeReference for every line containing 'TODO' in the given
    git ref (e.g. a commit SHA).

    Uses ``git grep`` to scan the ref without checking it out.
    """
    if not _COMMIT_SHA_REGEX.match(commit_ref):
        raise ValueError(
            f"commit_ref must be a hex SHA (4-40 characters), got: {commit_ref!r}"
        )
    res = subprocess.run(  # nosec: B603
        ["git", "grep", "-In", "TODO", commit_ref, "--"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if res.returncode == 1:
        return []
    if res.returncode != 0:
        raise RuntimeError(
            f"git grep failed (exit {res.returncode}): {res.stderr.decode()}"
        )
    prefix = f"{commit_ref}:"
    code_refs: List[CodeReference] = []
    for line in res.stdout.decode().splitlines():
        filepath, lineno, line_text = line.removeprefix(prefix).split(":", 2)
        code_refs.append(
            CodeReference(
                filepath=filepath,
                line_number=int(lineno),
                line_text=line_text,
            )
        )
    return code_refs


def get_entire_codebase_issue_references(
    commit_ref: str,
) -> Dict[Issue, List[CodeReference]]:
    """Scans every file tracked by git for TODO comments and returns a mapping
    from each referenced issue to the code locations where it appears."""
    issue_references: Dict[Issue, List[CodeReference]] = DefaultDict(list)

    for code_ref in _find_todo_code_references(commit_ref):
        for todo_match in _TODO_WITH_ARGS_REGEX.finditer(code_ref.line_text):
            try:
                issue = Issue.from_todo(todo_match.group(0))
            except ValueError:
                continue
            issue_references[issue].append(code_ref)

    return issue_references


def parse_issue_string(issue_string: str) -> Issue:
    """Parses '#123', 'Owner/Repo#123', or 'OBT-12345' into an Issue."""
    if re.fullmatch(LinearIssue.issue_regex(), issue_string):
        return LinearIssue.from_string(issue_string)
    return GithubIssue.from_string(issue_string)


def to_markdown(issue_references: Mapping[Issue, List[CodeReference]]) -> str:
    lines = []
    for issue in sorted(issue_references.keys(), key=str):
        lines.append(f"* {issue}")
        for reference in sorted(issue_references[issue]):
            lines.append(f"  * {reference}")
    return "\n".join(lines)


# --- Legacy functions used by recidiviz/tools/github/find_todos.py ---


TO_DO_REGEX = rf"TODO\({GithubIssue.issue_regex()}\)"


def _read_todo_lines_from_codebase() -> List[str]:
    res = subprocess.run(
        "git ls-files -z | xargs -0 grep -Il 'TODO' | tr '\n' '\\0' | xargs -0 grep -n 'TODO'",
        shell=True,
        stdout=subprocess.PIPE,
        check=True,
    )
    return res.stdout.decode().splitlines()


# TODO(#80248): Delete get_issue_references() once find_todos.py is replaced by
# find_closing_issue_todos.py.
def get_issue_references() -> Dict[Issue, List[CodeReference]]:
    todo_regex = re.compile(TO_DO_REGEX)
    issue_references: Dict[Issue, List[CodeReference]] = DefaultDict(list)

    for todo_line in _read_todo_lines_from_codebase():
        filepath, lineno, line_text = todo_line.split(":", 2)
        match = re.search(todo_regex, line_text)

        if match is not None:
            issue_references[
                GithubIssue(
                    repo=match.group("repo") or DEFAULT_REPO,
                    number=int(match.group("issue")),
                )
            ].append(
                CodeReference(
                    line_text=line_text,
                    filepath=filepath,
                    line_number=int(lineno),
                )
            )
    return issue_references
