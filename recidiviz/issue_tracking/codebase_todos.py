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

from recidiviz.github.github_code_reference import GithubCodeReference
from recidiviz.issue_tracking.issue import Issue
from recidiviz.issue_tracking.issue_parsing import TODO_ISSUE_REF_REGEX, issue_from_todo
from recidiviz.tools.utils.git_manager import get_local_repo_name

_COMMIT_SHA_REGEX = re.compile(r"^[0-9a-f]{4,40}$")


def _find_todo_code_references(
    commit_ref: str,
) -> List[GithubCodeReference]:
    """Returns a GithubCodeReference for every line containing 'TODO' in the
    locally checked out repo at the given git ref (e.g. a commit SHA).

    Uses ``git grep`` to scan the ref without checking it out.
    """
    if not _COMMIT_SHA_REGEX.match(commit_ref):
        raise ValueError(
            f"commit_ref must be a hex SHA (4-40 characters), got: {commit_ref!r}"
        )
    res = subprocess.run(  # nosec: B603, B607
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
    repo = get_local_repo_name()
    prefix = f"{commit_ref}:"
    code_refs: List[GithubCodeReference] = []
    for line in res.stdout.decode().splitlines():
        filepath, lineno, line_text = line.removeprefix(prefix).split(":", 2)
        code_refs.append(
            GithubCodeReference(
                repo=repo,
                filepath=filepath,
                line_number=int(lineno),
                line_text=line_text,
            )
        )
    return code_refs


def get_entire_codebase_issue_references(
    commit_ref: str,
) -> Dict[Issue, List[GithubCodeReference]]:
    """Scans every file tracked by git in the local repo for TODO comments and returns
    a mapping from each referenced issue to the code locations where it appears.
    """
    repo = get_local_repo_name()
    issue_references: Dict[Issue, List[GithubCodeReference]] = DefaultDict(list)

    for code_ref in _find_todo_code_references(commit_ref):
        for todo_match in TODO_ISSUE_REF_REGEX.finditer(code_ref.line_text):
            try:
                issue = issue_from_todo(todo_match.group(0), default_repo=repo)
            except ValueError:
                continue
            issue_references[issue].append(code_ref)

    return issue_references


def to_markdown(
    issue_references: Mapping[Issue, List[GithubCodeReference]],
) -> str:
    lines = []
    for issue in sorted(issue_references.keys(), key=str):
        lines.append(f"* {issue}")
        for reference in sorted(issue_references[issue]):
            lines.append(f"  * {reference}")
    return "\n".join(lines)
