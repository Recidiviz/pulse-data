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
"""Tests for recidiviz.repo.issue."""

import unittest

from recidiviz.repo.issue import GithubIssue, Issue, LinearIssue, UrlIssue


class TestGithubIssue(unittest.TestCase):
    """Tests for GithubIssue."""

    def test_from_string_short(self) -> None:
        issue = GithubIssue.from_string("#123")
        self.assertEqual(issue.repo, "Recidiviz/pulse-data")
        self.assertEqual(issue.number, 123)

    def test_from_string_with_repo(self) -> None:
        issue = GithubIssue.from_string("Recidiviz/pulse-dashboard#456")
        self.assertEqual(issue.repo, "Recidiviz/pulse-dashboard")
        self.assertEqual(issue.number, 456)

    def test_from_string_invalid(self) -> None:
        with self.assertRaises(ValueError):
            GithubIssue.from_string("not-an-issue")

    def test_str(self) -> None:
        issue = GithubIssue(repo="Recidiviz/pulse-data", number=123)
        self.assertEqual(str(issue), "Recidiviz/pulse-data#123")

    def test_from_url(self) -> None:
        issue = GithubIssue.from_url(
            "https://github.com/Recidiviz/pulse-data/issues/123"
        )
        self.assertEqual(issue.repo, "Recidiviz/pulse-data")
        self.assertEqual(issue.number, 123)

    def test_from_url_invalid(self) -> None:
        with self.assertRaises(ValueError):
            GithubIssue.from_url("not-a-url")

    def test_url_property(self) -> None:
        issue = GithubIssue(repo="Recidiviz/pulse-data", number=123)
        self.assertEqual(
            issue.url, "https://github.com/Recidiviz/pulse-data/issues/123"
        )

    def test_todo_regex(self) -> None:
        self.assertRegex("TODO(#123)", GithubIssue.todo_regex())
        self.assertRegex("TODO(Recidiviz/pulse-data#456)", GithubIssue.todo_regex())
        self.assertNotRegex("TODO(OBT-12345)", GithubIssue.todo_regex())
        self.assertNotRegex("TODO(XXXX)", GithubIssue.todo_regex())


class TestLinearIssue(unittest.TestCase):
    """Tests for LinearIssue."""

    def test_from_string(self) -> None:
        issue = LinearIssue.from_string("OBT-12345")
        self.assertEqual(issue.team_prefix, "OBT")
        self.assertEqual(issue.number, 12345)
        self.assertEqual(str(issue), "OBT-12345")

    def test_from_string_invalid(self) -> None:
        with self.assertRaises(ValueError):
            LinearIssue.from_string("#123")
        with self.assertRaises(ValueError):
            LinearIssue.from_string("XXXX")

    def test_issue_identifier(self) -> None:
        issue = LinearIssue(team_prefix="OBT", number=789)
        self.assertEqual(issue.issue_identifier, "OBT-789")

    def test_todo_regex(self) -> None:
        self.assertRegex("TODO(OBT-12345)", LinearIssue.todo_regex())
        self.assertRegex("TODO(ENG-1)", LinearIssue.todo_regex())
        self.assertNotRegex("TODO(#123)", LinearIssue.todo_regex())
        self.assertNotRegex("TODO(XXXX)", LinearIssue.todo_regex())


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
