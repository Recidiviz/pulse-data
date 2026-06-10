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
"""Tests for recidiviz.issue_tracking.issue_parsing."""

import unittest

from recidiviz.github.github_constants import RECIDIVIZ_DATA_REPO
from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.issue import UrlIssue
from recidiviz.issue_tracking.issue_parsing import issue_from_todo, parse_issue_string
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue


class ParseIssueStringTest(unittest.TestCase):
    """Tests for parse_issue_string()."""

    def test_github_hash(self) -> None:
        issue = parse_issue_string("#123", default_repo=RECIDIVIZ_DATA_REPO)
        self.assertEqual(
            issue, GithubIssue(repo="Recidiviz/pulse-data", number=123)
        )

    def test_github_repo(self) -> None:
        issue = parse_issue_string(
            "Recidiviz/pulse-data#789", default_repo=RECIDIVIZ_DATA_REPO
        )
        self.assertEqual(issue, GithubIssue(repo="Recidiviz/pulse-data", number=789))

    def test_linear(self) -> None:
        issue = parse_issue_string("OBT-12345", default_repo=RECIDIVIZ_DATA_REPO)
        self.assertEqual(issue, LinearIssue(team_prefix="OBT", number=12345))


class IssueFromTodoTest(unittest.TestCase):
    """Tests for issue_from_todo()."""

    def test_github_todo(self) -> None:
        issue = issue_from_todo("TODO(#123)", default_repo=RECIDIVIZ_DATA_REPO)
        self.assertEqual(
            issue, GithubIssue(repo="Recidiviz/pulse-data", number=123)
        )

    def test_linear_todo(self) -> None:
        issue = issue_from_todo("TODO(OBT-789)", default_repo=RECIDIVIZ_DATA_REPO)
        self.assertEqual(issue, LinearIssue(team_prefix="OBT", number=789))

    def test_url_todo(self) -> None:
        issue = issue_from_todo(
            "TODO(https://issues.apache.org/jira/browse/BEAM-12641)",
            default_repo=RECIDIVIZ_DATA_REPO,
        )
        self.assertEqual(
            issue, UrlIssue(url="https://issues.apache.org/jira/browse/BEAM-12641")
        )

    def test_unrecognized_todo(self) -> None:
        with self.assertRaises(ValueError):
            issue_from_todo("TODO(XXXX)", default_repo=RECIDIVIZ_DATA_REPO)
