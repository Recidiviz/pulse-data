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
"""Tests for find_closing_issue_todos."""

import unittest

from github import GithubException
from mock import MagicMock, patch

from recidiviz.github.github_code_reference import GithubCodeReference
from recidiviz.github.github_constants import RECIDIVIZ_DATA_REPO
from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.linear.linear_client import LinearApiError
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue
from recidiviz.tools.github.find_closing_issue_todos import main

FAKE_ISSUE_REFERENCES = {
    GithubIssue(repo="Recidiviz/pulse-data", number=123): [
        GithubCodeReference(
            repo=RECIDIVIZ_DATA_REPO,
            filepath="foo.py",
            line_number=10,
            line_text="# TODO(#123)",
        ),
    ],
    LinearIssue(team_prefix="OBT", number=456): [
        GithubCodeReference(
            repo=RECIDIVIZ_DATA_REPO,
            filepath="bar.py",
            line_number=20,
            line_text="# TODO(OBT-456)",
        ),
    ],
    GithubIssue(repo="Recidiviz/pulse-data", number=789): [
        GithubCodeReference(
            repo=RECIDIVIZ_DATA_REPO,
            filepath="baz.py",
            line_number=30,
            line_text="# TODO(#789)",
        ),
    ],
}

FAKE_PR_URL = "https://github.com/Recidiviz/pulse-data/pull/100"


@patch(
    "recidiviz.tools.github.find_closing_issue_todos.get_entire_codebase_issue_references"
)
@patch(
    "recidiviz.tools.github.find_closing_issue_todos.LinearClient",
)
@patch(
    "recidiviz.tools.github.find_closing_issue_todos.get_closing_github_issues",
    return_value=[],
)
@patch(
    "recidiviz.tools.github.find_closing_issue_todos.get_pr_head_sha",
    return_value="abc123def",
)
@patch(
    "recidiviz.tools.github.find_closing_issue_todos.Github",
)
class MainTest(unittest.TestCase):
    """Tests for find_closing_issue_todos.main()."""

    def _setup_linear_client(
        self,
        mock_linear_client_cls: MagicMock,
        *,
        closing_issues: list[LinearIssue] | None = None,
        resolve_linear_to_github_return: GithubIssue | None = None,
        resolve_github_to_linear_return: LinearIssue | None = None,
        get_closing_issues_side_effect: Exception | None = None,
    ) -> MagicMock:
        mock_client = MagicMock()
        mock_linear_client_cls.return_value = mock_client
        if get_closing_issues_side_effect:
            mock_client.get_closing_issues.side_effect = get_closing_issues_side_effect
        else:
            mock_client.get_closing_issues.return_value = closing_issues or []
        mock_client.resolve_linear_to_github.return_value = (
            resolve_linear_to_github_return
        )
        mock_client.resolve_github_to_linear.return_value = (
            resolve_github_to_linear_return
        )
        return mock_client

    def test_no_closing_issues(
        self,
        _mock_github_cls: MagicMock,
        _mock_get_pr_head_sha: MagicMock,
        _mock_gh_closing: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        self._setup_linear_client(mock_linear_client_cls)
        result = main(
            pr_url=FAKE_PR_URL,
            github_token="fake_gh_token",
            linear_api_key="fake_key",
        )
        self.assertEqual(result, 0)

    def test_github_closing_issue_found(
        self,
        _mock_github_cls: MagicMock,
        _mock_get_pr_head_sha: MagicMock,
        mock_gh_closing: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        self._setup_linear_client(mock_linear_client_cls)
        mock_gh_closing.return_value = [
            GithubIssue(repo="Recidiviz/pulse-data", number=123)
        ]
        result = main(
            pr_url=FAKE_PR_URL,
            github_token="fake_gh_token",
            linear_api_key="fake_key",
        )
        self.assertEqual(result, 1)

    def test_linear_closing_issue_found(
        self,
        _mock_github_cls: MagicMock,
        _mock_get_pr_head_sha: MagicMock,
        _mock_gh_closing: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        self._setup_linear_client(
            mock_linear_client_cls,
            closing_issues=[LinearIssue(team_prefix="OBT", number=456)],
        )
        result = main(
            pr_url=FAKE_PR_URL,
            github_token="fake_gh_token",
            linear_api_key="fake_key",
        )
        self.assertEqual(result, 1)

    def test_no_matching_closing_issues(
        self,
        _mock_github_cls: MagicMock,
        _mock_get_pr_head_sha: MagicMock,
        mock_gh_closing: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        self._setup_linear_client(mock_linear_client_cls)
        mock_gh_closing.return_value = [
            GithubIssue(repo="Recidiviz/pulse-data", number=999)
        ]
        result = main(
            pr_url=FAKE_PR_URL,
            github_token="fake_gh_token",
            linear_api_key="fake_key",
        )
        self.assertEqual(result, 0)

    def test_linked_issues_from_both_sources_are_deduplicated(
        self,
        _mock_github_cls: MagicMock,
        _mock_get_pr_head_sha: MagicMock,
        mock_gh_closing: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        self._setup_linear_client(
            mock_linear_client_cls,
            closing_issues=[LinearIssue(team_prefix="OBT", number=456)],
            resolve_linear_to_github_return=GithubIssue(
                repo="Recidiviz/pulse-data", number=123
            ),
            resolve_github_to_linear_return=LinearIssue(team_prefix="OBT", number=456),
        )
        mock_gh_closing.return_value = [
            GithubIssue(repo="Recidiviz/pulse-data", number=123)
        ]

        result = main(
            pr_url=FAKE_PR_URL,
            github_token="fake_gh_token",
            linear_api_key="fake_key",
        )
        self.assertEqual(result, 1)

    def test_cross_reference_expands_set(
        self,
        _mock_github_cls: MagicMock,
        _mock_get_pr_head_sha: MagicMock,
        mock_gh_closing: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        self._setup_linear_client(
            mock_linear_client_cls,
            resolve_github_to_linear_return=LinearIssue(team_prefix="OBT", number=456),
        )
        mock_gh_closing.return_value = [
            GithubIssue(repo="Recidiviz/pulse-data", number=123)
        ]

        result = main(
            pr_url=FAKE_PR_URL,
            github_token="fake_gh_token",
            linear_api_key="fake_key",
        )
        self.assertEqual(result, 1)

    def test_fails_closed_on_linear_api_error(
        self,
        _mock_github_cls: MagicMock,
        _mock_get_pr_head_sha: MagicMock,
        _mock_gh_closing: MagicMock,
        mock_linear_client_cls: MagicMock,
        _mock_refs: MagicMock,
    ) -> None:
        self._setup_linear_client(
            mock_linear_client_cls,
            get_closing_issues_side_effect=LinearApiError("API unreachable"),
        )
        with self.assertRaises(LinearApiError):
            main(
                pr_url=FAKE_PR_URL,
                github_token="fake_gh_token",
                linear_api_key="fake_key",
            )

    def test_fails_closed_on_github_api_error(
        self,
        _mock_github_cls: MagicMock,
        _mock_get_pr_head_sha: MagicMock,
        mock_gh_closing: MagicMock,
        mock_linear_client_cls: MagicMock,
        _mock_refs: MagicMock,
    ) -> None:
        self._setup_linear_client(mock_linear_client_cls)
        mock_gh_closing.side_effect = GithubException(
            502, {"message": "Bad Gateway"}, None
        )
        with self.assertRaises(GithubException):
            main(
                pr_url=FAKE_PR_URL,
                github_token="fake_gh_token",
                linear_api_key="fake_key",
            )
