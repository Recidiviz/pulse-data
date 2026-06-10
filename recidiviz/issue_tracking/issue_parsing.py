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
"""Lightweight helpers for parsing issue references (e.g. 'TODO(#123)',
'#123', 'OBT-456') into Issue objects.

This module intentionally depends only on the Issue classes so it stays cheap
to import from anywhere (including code reachable from Airflow DAG parsing);
heavier TODO-discovery tooling lives in codebase_todos.py.
"""

import re

from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.issue import Issue, UrlIssue
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue

TODO_ISSUE_REF_REGEX = re.compile(r"TODO\((?P<issue_ref>[^)]+)\)")


def issue_from_todo(todo_string: str, *, default_repo: str) -> Issue:
    """Parses a TODO string like 'TODO(#12345)' or 'TODO(OBT-789)' into the
    appropriate Issue subclass.
    """
    ref_match = re.fullmatch(TODO_ISSUE_REF_REGEX, todo_string)
    if ref_match is None:
        raise ValueError(f"Unrecognized TODO format: [{todo_string}]")
    issue_ref = ref_match.group("issue_ref")

    if re.fullmatch(GithubIssue.issue_regex(), issue_ref):
        return GithubIssue.from_string(issue_ref, default_repo=default_repo)
    if re.fullmatch(LinearIssue.issue_regex(), issue_ref):
        return LinearIssue.from_string(issue_ref)
    if re.fullmatch(UrlIssue.issue_regex(), issue_ref):
        return UrlIssue.from_string(issue_ref)

    raise ValueError(f"Unrecognized TODO format: [{todo_string}]")


def parse_issue_string(issue_string: str, *, default_repo: str) -> Issue:
    """Parses '#123', 'Owner/Repo#123', or 'OBT-12345' into an Issue."""
    if re.fullmatch(LinearIssue.issue_regex(), issue_string):
        return LinearIssue.from_string(issue_string)
    return GithubIssue.from_string(issue_string, default_repo=default_repo)
