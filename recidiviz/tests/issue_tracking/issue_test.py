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
"""Tests for recidiviz.issue_tracking.issue."""

import unittest

from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.issue import Issue, UrlIssue
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue


class TestUrlIssue(unittest.TestCase):
    """Tests for UrlIssue."""

    def test_from_string(self) -> None:
        issue = UrlIssue.from_string("https://issues.apache.org/jira/browse/BEAM-12641")
        self.assertEqual(issue.url, "https://issues.apache.org/jira/browse/BEAM-12641")
        self.assertEqual(str(issue), "https://issues.apache.org/jira/browse/BEAM-12641")

    def test_from_string_invalid(self) -> None:
        with self.assertRaises(ValueError):
            UrlIssue.from_string("not-a-url")

    def test_todo_regex(self) -> None:
        self.assertRegex(
            "TODO(https://issues.apache.org/jira/browse/BEAM-12641)",
            UrlIssue.todo_regex(),
        )
        self.assertNotRegex("TODO(#123)", UrlIssue.todo_regex())
        self.assertNotRegex("TODO(XXXX)", UrlIssue.todo_regex())


class TestIssueFromTodo(unittest.TestCase):
    """Tests for Issue.from_todo()."""

    def test_github_todo(self) -> None:
        issue = Issue.from_todo("TODO(#123)")
        self.assertEqual(
            issue, GithubIssue(repo="Recidiviz/pulse-data", number=123)
        )

    def test_linear_todo(self) -> None:
        issue = Issue.from_todo("TODO(OBT-789)")
        self.assertEqual(issue, LinearIssue(team_prefix="OBT", number=789))

    def test_url_todo(self) -> None:
        issue = Issue.from_todo(
            "TODO(https://issues.apache.org/jira/browse/BEAM-12641)"
        )
        self.assertEqual(
            issue, UrlIssue(url="https://issues.apache.org/jira/browse/BEAM-12641")
        )

    def test_unrecognized_todo(self) -> None:
        with self.assertRaises(ValueError):
            Issue.from_todo("TODO(XXXX)")
