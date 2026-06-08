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
"""Tests for reopen_closed_todo_issues."""

import unittest

from mock import MagicMock, call, patch

from recidiviz.github.github_code_reference import GithubCodeReference
from recidiviz.github.github_constants import RECIDIVIZ_DATA_REPO
from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.linear.linear_client import (
    LinearApiError,
    LinearIssueInfo,
)
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue
from recidiviz.tools.github.reopen_closed_todo_issues import build_reopen_comment, main

FAKE_HEAD_SHA = "abc123def456"

FAKE_ISSUE_REFERENCES = {
    LinearIssue(team_prefix="OBT", number=100): [
        GithubCodeReference(
            repo=RECIDIVIZ_DATA_REPO,
            filepath="foo.py",
            line_number=10,
            line_text="# TODO(OBT-100)",
        ),
    ],
    GithubIssue(repo="Recidiviz/pulse-data", number=200): [
        GithubCodeReference(
            repo=RECIDIVIZ_DATA_REPO,
            filepath="bar.py",
            line_number=20,
            line_text="# TODO(#200)",
        ),
    ],
    LinearIssue(team_prefix="ENG", number=300): [
        GithubCodeReference(
            repo=RECIDIVIZ_DATA_REPO,
            filepath="baz.py",
            line_number=30,
            line_text="# TODO(ENG-300)",
        ),
        GithubCodeReference(
            repo=RECIDIVIZ_DATA_REPO,
            filepath="qux.py",
            line_number=40,
            line_text="# TODO(ENG-300)",
        ),
    ],
}

OBT_100_INFO = LinearIssueInfo(
    linear_issue=LinearIssue(team_prefix="OBT", number=100),
    uuid="uuid-obt-100",
    title="Fix the widget",
)

ENG_300_INFO = LinearIssueInfo(
    linear_issue=LinearIssue(team_prefix="ENG", number=300),
    uuid="uuid-eng-300",
    title="Refactor pipeline",
)

OBT_999_INFO = LinearIssueInfo(
    linear_issue=LinearIssue(team_prefix="OBT", number=999),
    uuid="uuid-obt-999",
    title="No TODOs for this one",
)


class BuildReopenCommentTest(unittest.TestCase):
    """Tests for build_reopen_comment."""

    def test_single_reference(self) -> None:
        comment = build_reopen_comment(
            [
                GithubCodeReference(
                    repo=RECIDIVIZ_DATA_REPO,
                    filepath="foo.py",
                    line_number=10,
                    line_text="# TODO(OBT-100)",
                )
            ],
            commit_sha=FAKE_HEAD_SHA,
        )
        self.assertEqual(
            comment,
            """\
This issue was automatically reopened because the following TODOs still reference it in the codebase:

* [`foo.py:10`](https://github.com/Recidiviz/pulse-data/blob/abc123def456/foo.py#L10)

Before closing this issue, please either:
- Address the TODOs and remove them from the codebase, or
- Re-point the TODOs to a different tracking issue.

_Posted by [reopen-closed-todo-issues](https://github.com/Recidiviz/pulse-data/actions/workflows/reopen-closed-todo-issues.yml) GitHub Action._""",
        )

    def test_multiple_references_sorted(self) -> None:
        comment = build_reopen_comment(
            [
                GithubCodeReference(
                    repo=RECIDIVIZ_DATA_REPO,
                    filepath="z.py",
                    line_number=5,
                    line_text="# TODO(OBT-100)",
                ),
                GithubCodeReference(
                    repo=RECIDIVIZ_DATA_REPO,
                    filepath="a.py",
                    line_number=1,
                    line_text="# TODO(OBT-100)",
                ),
            ],
            commit_sha=FAKE_HEAD_SHA,
        )
        self.assertEqual(
            comment,
            """\
This issue was automatically reopened because the following TODOs still reference it in the codebase:

* [`a.py:1`](https://github.com/Recidiviz/pulse-data/blob/abc123def456/a.py#L1)
* [`z.py:5`](https://github.com/Recidiviz/pulse-data/blob/abc123def456/z.py#L5)

Before closing this issue, please either:
- Address the TODOs and remove them from the codebase, or
- Re-point the TODOs to a different tracking issue.

_Posted by [reopen-closed-todo-issues](https://github.com/Recidiviz/pulse-data/actions/workflows/reopen-closed-todo-issues.yml) GitHub Action._""",
        )


@patch(
    "recidiviz.tools.github.reopen_closed_todo_issues.get_entire_codebase_issue_references"
)
@patch("recidiviz.tools.github.reopen_closed_todo_issues.LinearClient")
@patch("recidiviz.tools.github.reopen_closed_todo_issues.GitManager")
class MainTest(unittest.TestCase):
    """Tests for reopen_closed_todo_issues.main()."""

    def _setup_mocks(
        self,
        mock_git_manager_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        *,
        completed_issues: list[LinearIssueInfo] | None = None,
        resolve_linear_to_github_map: dict[LinearIssue, GithubIssue] | None = None,
    ) -> MagicMock:
        mock_git_manager_cls.return_value.get_head_sha.return_value = FAKE_HEAD_SHA

        mock_client = MagicMock()
        mock_linear_client_cls.return_value = mock_client
        mock_client.get_recently_closed_issues.return_value = completed_issues or []
        resolve_map = resolve_linear_to_github_map or {}
        mock_client.resolve_linear_to_github.side_effect = resolve_map.get
        return mock_client

    def test_no_recently_completed_issues(
        self,
        mock_git_manager_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        self._setup_mocks(mock_git_manager_cls, mock_linear_client_cls)
        result = main(linear_api_key="fake_key", lookback_minutes=70, dry_run=False)
        self.assertEqual(result, 0)

    def test_completed_issue_with_matching_todo_is_reopened(
        self,
        mock_git_manager_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        mock_client = self._setup_mocks(
            mock_git_manager_cls,
            mock_linear_client_cls,
            completed_issues=[OBT_100_INFO],
        )
        result = main(linear_api_key="fake_key", lookback_minutes=70, dry_run=False)
        self.assertEqual(result, 0)
        mock_client.reopen_issue.assert_called_once_with(OBT_100_INFO)
        mock_client.create_comment.assert_called_once()

    def test_completed_issue_without_matching_todo(
        self,
        mock_git_manager_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        mock_client = self._setup_mocks(
            mock_git_manager_cls,
            mock_linear_client_cls,
            completed_issues=[OBT_999_INFO],
        )
        result = main(linear_api_key="fake_key", lookback_minutes=70, dry_run=False)
        self.assertEqual(result, 0)
        mock_client.reopen_issue.assert_not_called()

    def test_cross_reference_catches_github_format_todo(
        self,
        mock_git_manager_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        """A Linear issue with no direct TODO match but whose synced GitHub
        issue has a TODO(#200) should still be caught."""
        no_linear_todo_info = LinearIssueInfo(
            linear_issue=LinearIssue(team_prefix="OBT", number=555),
            uuid="uuid-obt-555",
            title="Cross-ref test",
        )
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        mock_client = self._setup_mocks(
            mock_git_manager_cls,
            mock_linear_client_cls,
            completed_issues=[no_linear_todo_info],
            resolve_linear_to_github_map={
                LinearIssue(team_prefix="OBT", number=555): GithubIssue(
                    repo="Recidiviz/pulse-data", number=200
                ),
            },
        )
        result = main(linear_api_key="fake_key", lookback_minutes=70, dry_run=False)
        self.assertEqual(result, 0)
        mock_client.reopen_issue.assert_called_once_with(no_linear_todo_info)

    def test_multiple_issues_partially_matched(
        self,
        mock_git_manager_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        mock_client = self._setup_mocks(
            mock_git_manager_cls,
            mock_linear_client_cls,
            completed_issues=[OBT_100_INFO, OBT_999_INFO, ENG_300_INFO],
        )
        result = main(linear_api_key="fake_key", lookback_minutes=70, dry_run=False)
        self.assertEqual(result, 0)
        self.assertEqual(mock_client.reopen_issue.call_count, 2)
        mock_client.reopen_issue.assert_has_calls(
            [call(OBT_100_INFO), call(ENG_300_INFO)]
        )

    def test_linear_api_error_propagates(
        self,
        mock_git_manager_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        mock_client = self._setup_mocks(mock_git_manager_cls, mock_linear_client_cls)
        mock_client.get_recently_closed_issues.side_effect = LinearApiError(
            "API unreachable"
        )
        with self.assertRaises(LinearApiError):
            main(linear_api_key="fake_key", lookback_minutes=70, dry_run=False)
        mock_client.reopen_issue.assert_not_called()

    def test_individual_reopen_failure_continues(
        self,
        mock_git_manager_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        mock_client = self._setup_mocks(
            mock_git_manager_cls,
            mock_linear_client_cls,
            completed_issues=[OBT_100_INFO, ENG_300_INFO],
        )
        mock_client.reopen_issue.side_effect = [
            LinearApiError("Failed"),
            None,
        ]
        result = main(linear_api_key="fake_key", lookback_minutes=70, dry_run=False)
        self.assertEqual(result, 1)
        self.assertEqual(mock_client.reopen_issue.call_count, 2)

    def test_comment_contains_file_paths(
        self,
        mock_git_manager_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_refs: MagicMock,
    ) -> None:
        mock_refs.return_value = FAKE_ISSUE_REFERENCES
        mock_client = self._setup_mocks(
            mock_git_manager_cls,
            mock_linear_client_cls,
            completed_issues=[ENG_300_INFO],
        )
        main(linear_api_key="fake_key", lookback_minutes=70, dry_run=False)
        comment_body = mock_client.create_comment.call_args[0][1]
        self.assertIn("baz.py:30", comment_body)
        self.assertIn("qux.py:40", comment_body)
