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
"""Tests for auto_link_linear_prs."""

import unittest

from mock import MagicMock, call, patch

from recidiviz.github.github_issue import GithubIssue
from recidiviz.github.github_pull_request import GithubPullRequest
from recidiviz.issue_tracking.linear.linear_client import (
    LinearApiError,
    LinearAttachment,
)
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue
from recidiviz.issue_tracking.linear.linear_types import LinkKind
from recidiviz.tools.github.auto_link_linear_prs import (
    AUTO_LINK_SOURCE_MARKER,
    main,
    parse_issue_references,
)

FAKE_PR_URL = "https://github.com/Recidiviz/pulse-data/pull/100"
FAKE_PR = GithubPullRequest(owner="Recidiviz", repo="pulse-data", number=100)


def _gi(number: int, repo: str = "Recidiviz/pulse-data") -> GithubIssue:
    return GithubIssue(repo=repo, number=number)


class ParseIssueReferencesTest(unittest.TestCase):
    """Tests for parse_issue_references()."""

    def test_closing_keyword(self) -> None:
        self.assertEqual(
            parse_issue_references("Closes #123", FAKE_PR),
            {LinkKind.CLOSES: [_gi(123)]},
        )

    def test_fix_keyword(self) -> None:
        self.assertEqual(
            parse_issue_references("Fixes #456", FAKE_PR),
            {LinkKind.CLOSES: [_gi(456)]},
        )

    def test_resolve_keyword(self) -> None:
        self.assertEqual(
            parse_issue_references("Resolves #789", FAKE_PR),
            {LinkKind.CLOSES: [_gi(789)]},
        )

    def test_non_closing_keyword(self) -> None:
        self.assertEqual(
            parse_issue_references("Part of #123", FAKE_PR),
            {LinkKind.CONTRIBUTES: [_gi(123)]},
        )

    def test_related_to_keyword(self) -> None:
        self.assertEqual(
            parse_issue_references("Related to #456", FAKE_PR),
            {LinkKind.CONTRIBUTES: [_gi(456)]},
        )

    def test_contributes_to_keyword(self) -> None:
        self.assertEqual(
            parse_issue_references("Contributes to #789", FAKE_PR),
            {LinkKind.CONTRIBUTES: [_gi(789)]},
        )

    def test_bare_mention_ignored(self) -> None:
        self.assertEqual(parse_issue_references("See #123 for context", FAKE_PR), {})

    def test_case_insensitive(self) -> None:
        self.assertEqual(
            parse_issue_references("CLOSES #123", FAKE_PR),
            {LinkKind.CLOSES: [_gi(123)]},
        )
        self.assertEqual(
            parse_issue_references("part OF #456", FAKE_PR),
            {LinkKind.CONTRIBUTES: [_gi(456)]},
        )

    def test_multiple_references(self) -> None:
        result = parse_issue_references("Closes #123\nPart of #456", FAKE_PR)
        self.assertEqual(
            result, {LinkKind.CLOSES: [_gi(123)], LinkKind.CONTRIBUTES: [_gi(456)]}
        )

    def test_closing_wins_over_non_closing(self) -> None:
        result = parse_issue_references("Part of #123\nCloses #123", FAKE_PR)
        self.assertEqual(result, {LinkKind.CLOSES: [_gi(123)]})

    def test_with_repo_prefix(self) -> None:
        self.assertEqual(
            parse_issue_references("Closes Recidiviz/pulse-data#123", FAKE_PR),
            {LinkKind.CLOSES: [_gi(123)]},
        )

    def test_with_different_repo_prefix(self) -> None:
        self.assertEqual(
            parse_issue_references("Closes OtherOrg/other-repo#123", FAKE_PR),
            {LinkKind.CLOSES: [_gi(123, repo="OtherOrg/other-repo")]},
        )

    def test_empty_body(self) -> None:
        self.assertEqual(parse_issue_references("", FAKE_PR), {})

    def test_implements_keyword(self) -> None:
        self.assertEqual(
            parse_issue_references("Implements #123", FAKE_PR),
            {LinkKind.CONTRIBUTES: [_gi(123)]},
        )

    def test_for_keyword(self) -> None:
        self.assertEqual(
            parse_issue_references("For #123", FAKE_PR),
            {LinkKind.CONTRIBUTES: [_gi(123)]},
        )

    def test_keyword_at_start_of_line(self) -> None:
        self.assertEqual(
            parse_issue_references("Some text\nCloses #123", FAKE_PR),
            {LinkKind.CLOSES: [_gi(123)]},
        )

    def test_full_github_url(self) -> None:
        self.assertEqual(
            parse_issue_references(
                "Closes https://github.com/Recidiviz/pulse-data/issues/80250",
                FAKE_PR,
            ),
            {LinkKind.CLOSES: [_gi(80250)]},
        )

    def test_full_github_url_non_closing(self) -> None:
        self.assertEqual(
            parse_issue_references(
                "Part of https://github.com/Recidiviz/pulse-data/issues/80250",
                FAKE_PR,
            ),
            {LinkKind.CONTRIBUTES: [_gi(80250)]},
        )

    def test_mixed_short_and_url_references(self) -> None:
        body = (
            "Closes #123\n"
            "Part of https://github.com/Recidiviz/pulse-data/issues/456"
        )
        result = parse_issue_references(body, FAKE_PR)
        self.assertEqual(
            result, {LinkKind.CLOSES: [_gi(123)], LinkKind.CONTRIBUTES: [_gi(456)]}
        )

    def test_url_deduplicates_with_short_reference(self) -> None:
        body = (
            "Part of #123\n"
            "Closes https://github.com/Recidiviz/pulse-data/issues/123"
        )
        result = parse_issue_references(body, FAKE_PR)
        self.assertEqual(result, {LinkKind.CLOSES: [_gi(123)]})


@patch(
    "recidiviz.tools.github.auto_link_linear_prs.get_pr_body",
    return_value="",
)
@patch("recidiviz.tools.github.auto_link_linear_prs.LinearClient")
@patch("recidiviz.tools.github.auto_link_linear_prs.Github")
class MainTest(unittest.TestCase):
    """Tests for auto_link_linear_prs.main()."""

    def _setup(
        self,
        mock_linear_client_cls: MagicMock,
        mock_get_pr_body: MagicMock,
        *,
        pr_body: str = "",
        resolve_map: dict[int, LinearIssue | None] | None = None,
        all_attachments: list[LinearAttachment] | None = None,
    ) -> MagicMock:
        mock_get_pr_body.return_value = pr_body
        mock_client = MagicMock()
        mock_linear_client_cls.return_value = mock_client
        resolve_map = resolve_map or {}
        mock_client.resolve_github_to_linear.side_effect = lambda gi: resolve_map.get(
            gi.number
        )
        mock_client.get_all_pr_attachments.return_value = all_attachments or []
        mock_client.create_pr_attachment.return_value = "new-att-id"
        return mock_client

    def _run(self) -> int:
        return main(
            pr_url=FAKE_PR_URL,
            github_token="fake_gh_token",
            linear_api_key="key",
        )

    def test_no_references(
        self,
        _mock_github_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_get_pr_body: MagicMock,
    ) -> None:
        self._setup(
            mock_linear_client_cls, mock_get_pr_body, pr_body="No issue refs here"
        )
        self.assertEqual(self._run(), 0)

    def test_creates_link_for_closing_reference(
        self,
        _mock_github_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_get_pr_body: MagicMock,
    ) -> None:
        mock_client = self._setup(
            mock_linear_client_cls,
            mock_get_pr_body,
            pr_body="Closes #123",
            resolve_map={123: LinearIssue(team_prefix="OBT", number=456)},
        )
        self.assertEqual(self._run(), 0)
        mock_client.create_pr_attachment.assert_called_once_with(
            "OBT-456", FAKE_PR_URL, 100, LinkKind.CLOSES, AUTO_LINK_SOURCE_MARKER
        )
        mock_client.start_issue_if_unstarted.assert_called_once_with(
            LinearIssue(team_prefix="OBT", number=456)
        )

    def test_creates_link_for_non_closing_reference(
        self,
        _mock_github_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_get_pr_body: MagicMock,
    ) -> None:
        mock_client = self._setup(
            mock_linear_client_cls,
            mock_get_pr_body,
            pr_body="Part of #123",
            resolve_map={123: LinearIssue(team_prefix="OBT", number=456)},
        )
        self.assertEqual(self._run(), 0)
        mock_client.create_pr_attachment.assert_called_once_with(
            "OBT-456", FAKE_PR_URL, 100, LinkKind.CONTRIBUTES, AUTO_LINK_SOURCE_MARKER
        )
        mock_client.start_issue_if_unstarted.assert_called_once_with(
            LinearIssue(team_prefix="OBT", number=456)
        )

    def test_skips_issue_with_no_synced_linear_issue(
        self,
        _mock_github_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_get_pr_body: MagicMock,
    ) -> None:
        mock_client = self._setup(
            mock_linear_client_cls,
            mock_get_pr_body,
            pr_body="Closes #123",
            resolve_map={123: None},
        )
        self.assertEqual(self._run(), 0)
        mock_client.create_pr_attachment.assert_not_called()

    def test_skips_issue_already_linked_by_native_integration(
        self,
        _mock_github_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_get_pr_body: MagicMock,
    ) -> None:
        mock_client = self._setup(
            mock_linear_client_cls,
            mock_get_pr_body,
            pr_body="Closes #123",
            resolve_map={123: LinearIssue(team_prefix="OBT", number=456)},
            all_attachments=[
                LinearAttachment(
                    id="native-att",
                    issue_identifier="OBT-456",
                    link_kind=LinkKind.CLOSES,
                    source=None,
                )
            ],
        )
        self.assertEqual(self._run(), 0)
        mock_client.create_pr_attachment.assert_not_called()
        mock_client.start_issue_if_unstarted.assert_not_called()

    def test_deletes_attachment_when_reference_removed(
        self,
        _mock_github_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_get_pr_body: MagicMock,
    ) -> None:
        mock_client = self._setup(
            mock_linear_client_cls,
            mock_get_pr_body,
            pr_body="No more issue references",
            all_attachments=[
                LinearAttachment(
                    id="att-1",
                    issue_identifier="OBT-456",
                    link_kind=LinkKind.CLOSES,
                    source=AUTO_LINK_SOURCE_MARKER,
                )
            ],
        )
        self.assertEqual(self._run(), 0)
        mock_client.delete_attachment.assert_called_once_with("att-1")
        mock_client.start_issue_if_unstarted.assert_not_called()

    def test_updates_attachment_when_link_kind_changes(
        self,
        _mock_github_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_get_pr_body: MagicMock,
    ) -> None:
        mock_client = self._setup(
            mock_linear_client_cls,
            mock_get_pr_body,
            pr_body="Part of #123",
            resolve_map={123: LinearIssue(team_prefix="OBT", number=456)},
            all_attachments=[
                LinearAttachment(
                    id="att-1",
                    issue_identifier="OBT-456",
                    link_kind=LinkKind.CLOSES,
                    source=AUTO_LINK_SOURCE_MARKER,
                )
            ],
        )
        self.assertEqual(self._run(), 0)
        mock_client.update_attachment.assert_called_once_with(
            "att-1", "PR #100", LinkKind.CONTRIBUTES, AUTO_LINK_SOURCE_MARKER
        )
        mock_client.create_pr_attachment.assert_not_called()
        mock_client.start_issue_if_unstarted.assert_called_once_with(
            LinearIssue(team_prefix="OBT", number=456)
        )

    def test_no_change_when_attachment_matches(
        self,
        _mock_github_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_get_pr_body: MagicMock,
    ) -> None:
        mock_client = self._setup(
            mock_linear_client_cls,
            mock_get_pr_body,
            pr_body="Closes #123",
            resolve_map={123: LinearIssue(team_prefix="OBT", number=456)},
            all_attachments=[
                LinearAttachment(
                    id="att-1",
                    issue_identifier="OBT-456",
                    link_kind=LinkKind.CLOSES,
                    source=AUTO_LINK_SOURCE_MARKER,
                )
            ],
        )
        self.assertEqual(self._run(), 0)
        mock_client.create_pr_attachment.assert_not_called()
        mock_client.update_attachment.assert_not_called()
        mock_client.delete_attachment.assert_not_called()
        mock_client.start_issue_if_unstarted.assert_not_called()

    def test_multiple_references(
        self,
        _mock_github_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_get_pr_body: MagicMock,
    ) -> None:
        mock_client = self._setup(
            mock_linear_client_cls,
            mock_get_pr_body,
            pr_body="Closes #123\nPart of #789",
            resolve_map={
                123: LinearIssue(team_prefix="OBT", number=456),
                789: LinearIssue(team_prefix="OBT", number=101),
            },
        )
        self.assertEqual(self._run(), 0)
        self.assertEqual(mock_client.create_pr_attachment.call_count, 2)
        mock_client.create_pr_attachment.assert_has_calls(
            [
                call(
                    "OBT-456",
                    FAKE_PR_URL,
                    100,
                    LinkKind.CLOSES,
                    AUTO_LINK_SOURCE_MARKER,
                ),
                call(
                    "OBT-101",
                    FAKE_PR_URL,
                    100,
                    LinkKind.CONTRIBUTES,
                    AUTO_LINK_SOURCE_MARKER,
                ),
            ],
            any_order=True,
        )

    def test_raises_on_duplicate_action_created_attachments(
        self,
        _mock_github_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_get_pr_body: MagicMock,
    ) -> None:
        self._setup(
            mock_linear_client_cls,
            mock_get_pr_body,
            pr_body="Closes #123",
            resolve_map={123: LinearIssue(team_prefix="OBT", number=456)},
            all_attachments=[
                LinearAttachment(
                    id="att-1",
                    issue_identifier="OBT-456",
                    link_kind=LinkKind.CLOSES,
                    source=AUTO_LINK_SOURCE_MARKER,
                ),
                LinearAttachment(
                    id="att-2",
                    issue_identifier="OBT-456",
                    link_kind=LinkKind.CLOSES,
                    source=AUTO_LINK_SOURCE_MARKER,
                ),
            ],
        )
        with self.assertRaises(ValueError):
            self._run()

    def test_raises_on_linear_api_error(
        self,
        _mock_github_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_get_pr_body: MagicMock,
    ) -> None:
        mock_get_pr_body.return_value = "Closes #123"
        mock_client = MagicMock()
        mock_linear_client_cls.return_value = mock_client
        mock_client.resolve_github_to_linear.side_effect = LinearApiError(
            "API unreachable"
        )
        with self.assertRaises(LinearApiError):
            self._run()

    def test_creates_link_for_full_url_reference(
        self,
        _mock_github_cls: MagicMock,
        mock_linear_client_cls: MagicMock,
        mock_get_pr_body: MagicMock,
    ) -> None:
        mock_client = self._setup(
            mock_linear_client_cls,
            mock_get_pr_body,
            pr_body="Closes https://github.com/Recidiviz/pulse-data/issues/80250",
            resolve_map={80250: LinearIssue(team_prefix="OBT", number=999)},
        )
        self.assertEqual(self._run(), 0)
        mock_client.create_pr_attachment.assert_called_once_with(
            "OBT-999", FAKE_PR_URL, 100, LinkKind.CLOSES, AUTO_LINK_SOURCE_MARKER
        )
