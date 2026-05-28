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
"""Tests for recidiviz.github.github_issue."""

import unittest

from recidiviz.github.github_issue import GithubIssue


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
