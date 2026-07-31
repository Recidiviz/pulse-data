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
"""Helpers for working with GitHub/Linear two-way sync."""

from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.linear.linear_client import LinearClient
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue


def resolve_cross_references(
    issues: set[GithubIssue | LinearIssue],
    linear_client: LinearClient,
) -> set[GithubIssue | LinearIssue]:
    """Expand a set of issues by resolving cross-references between GitHub and
    Linear. For each GithubIssue, looks up the synced LinearIssue (and vice
    versa) and adds it to the returned set, along with any identifiers the
    Linear issue held before moving between teams.

    Previous identifiers are aliases of the same issue, so they are added to
    the output but never used as resolution inputs — their GitHub twin is the
    one already resolved from the current identifier."""
    expanded: set[GithubIssue | LinearIssue] = set(issues)
    for issue in issues:
        if isinstance(issue, GithubIssue):
            issue_group = linear_client.get_equivalent_issue_group_for_github_issue(
                issue
            )
        elif isinstance(issue, LinearIssue):
            issue_group = linear_client.get_equivalent_issue_group_for_linear_issue(
                issue
            )
        else:
            raise ValueError(f"Unexpected issue type: [{type(issue)}]")
        if issue_group is None:
            continue
        expanded.add(issue_group.linear_issue)
        expanded.update(issue_group.previous_issues)
        if issue_group.github_issue is not None:
            expanded.add(issue_group.github_issue)
    return expanded
