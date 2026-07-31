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
from recidiviz.issue_tracking.linear.linear_client import (
    LinearApiError,
    LinearEquivalentIssueGroup,
)
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
        linear_issue_group_map: dict[LinearIssue, LinearEquivalentIssueGroup]
        | None = None,
        github_issue_group_return: LinearEquivalentIssueGroup | None = None,
        get_closing_issues_side_effect: Exception | None = None,
    ) -> MagicMock:
        mock_client = MagicMock()
        mock_linear_client_cls.return_value = mock_client
        if get_closing_issues_side_effect:
            mock_client.get_closing_issues.side_effect = get_closing_issues_side_effect
        else:
            mock_client.get_closing_issues.return_value = closing_issues or []
        issue_group_map = linear_issue_group_map or {}
        mock_client.get_equivalent_issue_group_for_linear_issue.side_effect = (
            lambda linear_issue: issue_group_map.get(
                linear_issue,
                LinearEquivalentIssueGroup(
                    linear_issue=linear_issue, previous_issues=set(), github_issue=None
                ),
            )
        )
        mock_client.get_equivalent_issue_group_for_github_issue.return_value = (
            github_issue_group_return
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
        obt_456 = LinearIssue(team_prefix="OBT", number=456)
        gh_123 = GithubIssue(repo="Recidiviz/pulse-data", number=123)
        synced_pair_group = LinearEquivalentIssueGroup(
            linear_issue=obt_456, previous_issues=set(), github_issue=gh_123
        )
        self._setup_linear_client(
            mock_linear_client_cls,
            closing_issues=[obt_456],
            linear_issue_group_map={obt_456: synced_pair_group},
            github_issue_group_return=synced_pair_group,
        )
        mock_gh_closing.return_value = [gh_123]

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
        gh_123 = GithubIssue(repo="Recidiviz/pulse-data", number=123)
        self._setup_linear_client(
            mock_linear_client_cls,
            github_issue_group_return=LinearEquivalentIssueGroup(
                linear_issue=LinearIssue(team_prefix="OBT", number=456),
                previous_issues=set(),
                github_issue=gh_123,
            ),
        )
        mock_gh_closing.return_value = [gh_123]

        result = main(
            pr_url=FAKE_PR_URL,
            github_token="fake_gh_token",
            linear_api_key="fake_key",
        )
        self.assertEqual(result, 1)

    def test_linear_closing_issue_matches_todo_under_previous_identifier(
        self,
        _mock_github_cls: MagicMock,
        _mock_get_pr_head_sha: MagicMock,
        _mock_gh_closing: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        """A PR closing TN-94 (previously OBT-456) should flag a lingering
        TODO(OBT-456) even though the identifier changed on team move."""
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        tn_94 = LinearIssue(team_prefix="TN", number=94)
        self._setup_linear_client(
            mock_linear_client_cls,
            closing_issues=[tn_94],
            linear_issue_group_map={
                tn_94: LinearEquivalentIssueGroup(
                    linear_issue=tn_94,
                    previous_issues={LinearIssue(team_prefix="OBT", number=456)},
                    github_issue=None,
                ),
            },
        )
        result = main(
            pr_url=FAKE_PR_URL,
            github_token="fake_gh_token",
            linear_api_key="fake_key",
        )
        self.assertEqual(result, 1)

    def test_github_closing_issue_matches_todo_under_previous_linear_identifier(
        self,
        _mock_github_cls: MagicMock,
        _mock_get_pr_head_sha: MagicMock,
        mock_gh_closing: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        """A PR saying 'Closes #999' (the GitHub twin of TN-94, previously
        OBT-456) should flag a lingering TODO(OBT-456) — previous identifiers
        must come back on the GitHub-to-Linear resolution, since the resolved
        Linear issue is never itself re-resolved."""
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        gh_999 = GithubIssue(repo="Recidiviz/pulse-data", number=999)
        self._setup_linear_client(
            mock_linear_client_cls,
            github_issue_group_return=LinearEquivalentIssueGroup(
                linear_issue=LinearIssue(team_prefix="TN", number=94),
                previous_issues={LinearIssue(team_prefix="OBT", number=456)},
                github_issue=gh_999,
            ),
        )
        mock_gh_closing.return_value = [gh_999]
        result = main(
            pr_url=FAKE_PR_URL,
            github_token="fake_gh_token",
            linear_api_key="fake_key",
        )
        self.assertEqual(result, 1)

    def test_previous_identifiers_without_codebase_match(
        self,
        _mock_github_cls: MagicMock,
        _mock_get_pr_head_sha: MagicMock,
        _mock_gh_closing: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        tn_94 = LinearIssue(team_prefix="TN", number=94)
        self._setup_linear_client(
            mock_linear_client_cls,
            closing_issues=[tn_94],
            linear_issue_group_map={
                tn_94: LinearEquivalentIssueGroup(
                    linear_issue=tn_94,
                    previous_issues={LinearIssue(team_prefix="ZZZ", number=1)},
                    github_issue=None,
                ),
            },
        )
        result = main(
            pr_url=FAKE_PR_URL,
            github_token="fake_gh_token",
            linear_api_key="fake_key",
        )
        self.assertEqual(result, 0)

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
