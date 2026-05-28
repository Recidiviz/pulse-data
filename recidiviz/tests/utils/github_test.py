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
"""Tests for recidiviz.utils.github."""
import unittest
from unittest import mock

from github import GithubException

from recidiviz.repo.issue import GithubIssue
from recidiviz.utils.github import get_closing_github_issues, poll_for_pr_merge
from recidiviz.utils.github_pull_request import GithubPullRequest

GRAPHQL_RESPONSE_TWO_ISSUES = {
    "data": {
        "repository": {
            "pullRequest": {
                "closingIssuesReferences": {
                    "nodes": [
                        {
                            "repository": {"nameWithOwner": "Recidiviz/pulse-data"},
                            "number": 123,
                        },
                        {
                            "repository": {"nameWithOwner": "Recidiviz/pulse-data"},
                            "number": 456,
                        },
                    ]
                }
            }
        }
    }
}

GRAPHQL_RESPONSE_EMPTY: dict = {
    "data": {"repository": {"pullRequest": {"closingIssuesReferences": {"nodes": []}}}}
}


class TestGithubPullRequest(unittest.TestCase):
    """Tests for GithubPullRequest."""

    def test_from_url(self) -> None:
        pr = GithubPullRequest.from_url(
            "https://github.com/Recidiviz/pulse-data/pull/123"
        )
        self.assertEqual(pr.owner, "Recidiviz")
        self.assertEqual(pr.repo, "pulse-data")
        self.assertEqual(pr.number, 123)

    def test_url_property(self) -> None:
        pr = GithubPullRequest(owner="Recidiviz", repo="pulse-data", number=456)
        self.assertEqual(pr.url, "https://github.com/Recidiviz/pulse-data/pull/456")

    def test_from_url_roundtrip(self) -> None:
        url = "https://github.com/Recidiviz/pulse-dashboard/pull/789"
        pr = GithubPullRequest.from_url(url)
        self.assertEqual(pr.url, url)

    def test_from_url_invalid(self) -> None:
        with self.assertRaises(ValueError):
            GithubPullRequest.from_url("not-a-url")

    def test_from_url_rejects_issue_url(self) -> None:
        with self.assertRaises(ValueError):
            GithubPullRequest.from_url(
                "https://github.com/Recidiviz/pulse-data/issues/123"
            )


class TestGetClosingGithubIssues(unittest.TestCase):
    """Tests for get_closing_github_issues."""

    PR = GithubPullRequest(owner="Recidiviz", repo="pulse-data", number=100)

    def test_returns_closing_issues(self) -> None:
        mock_client = mock.MagicMock()
        mock_client.requester.graphql_query.return_value = (
            {},
            GRAPHQL_RESPONSE_TWO_ISSUES,
        )

        result = get_closing_github_issues(self.PR, mock_client)
        self.assertEqual(
            result,
            [
                GithubIssue(repo="Recidiviz/pulse-data", number=123),
                GithubIssue(repo="Recidiviz/pulse-data", number=456),
            ],
        )

    def test_returns_empty_when_no_closing_issues(self) -> None:
        mock_client = mock.MagicMock()
        mock_client.requester.graphql_query.return_value = (
            {},
            GRAPHQL_RESPONSE_EMPTY,
        )

        result = get_closing_github_issues(self.PR, mock_client)
        self.assertEqual(result, [])

    def test_raises_on_graphql_errors(self) -> None:
        mock_client = mock.MagicMock()
        mock_client.requester.graphql_query.side_effect = GithubException(
            400, {"errors": [{"message": "bad"}]}, None
        )

        with self.assertRaises(GithubException):
            get_closing_github_issues(self.PR, mock_client)


class TestPollForPrMerge(unittest.TestCase):
    """Tests for poll_for_pr_merge."""

    def setUp(self) -> None:
        self.mock_github_client = mock.MagicMock()

        self.patch_get_pr = mock.patch("recidiviz.utils.github.get_pr_if_exists")
        self.mock_get_pr = self.patch_get_pr.start()

        self.patch_time = mock.patch("recidiviz.utils.github.time")
        self.mock_time = self.patch_time.start()

    def tearDown(self) -> None:
        self.patch_get_pr.stop()
        self.patch_time.stop()

    def test_pr_merges(self) -> None:
        mock_pr_url = "https://github.com/Recidiviz/looker/pull/123"

        # First call: PR is open, second call: PR is merged (returns None)
        self.mock_get_pr.side_effect = [mock_pr_url, None]
        self.mock_time.time.side_effect = [0, 30, 60]

        poll_for_pr_merge(
            github_client=self.mock_github_client,
            pr_branch="update-lookml-sync-abc123d",
            base_branch="main",
            repo="Recidiviz/looker",
            timeout_minutes=5,
        )

        self.assertEqual(self.mock_get_pr.call_count, 2)
        self.mock_time.sleep.assert_called_once()

    def test_timeout_exceeded(self) -> None:
        mock_pr_url = "https://github.com/Recidiviz/looker/pull/123"

        self.mock_get_pr.return_value = mock_pr_url
        self.mock_time.time.side_effect = [0, 30, 60, 90]

        with self.assertRaisesRegex(TimeoutError, "Timed out after 1 minutes"):
            poll_for_pr_merge(
                github_client=self.mock_github_client,
                pr_branch="update-lookml-sync-abc123d",
                base_branch="main",
                repo="Recidiviz/looker",
                timeout_minutes=1,
            )
