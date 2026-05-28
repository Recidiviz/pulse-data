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
"""Tests for the Linear API client."""

import unittest

from mock import MagicMock, patch

from recidiviz.repo.issue import GithubIssue, LinearIssue
from recidiviz.repo.linear_client import (
    LinearApiError,
    LinearAttachment,
    LinearClient,
    LinkKind,
)

FAKE_API_KEY = "lin_api_test_key"


class GetClosingIssuesTest(unittest.TestCase):
    """Tests for LinearClient.get_closing_issues()."""

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_returns_closing_issues(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": {
                    "attachmentsForURL": {
                        "nodes": [
                            {
                                "metadata": {"linkKind": "closes"},
                                "issue": {"identifier": "OBT-123"},
                            },
                            {
                                "metadata": {"linkKind": "contributes"},
                                "issue": {"identifier": "OBT-456"},
                            },
                            {
                                "metadata": {"linkKind": "closes"},
                                "issue": {"identifier": "ENG-789"},
                            },
                        ]
                    }
                }
            },
        )

        client = LinearClient(FAKE_API_KEY)
        result = client.get_closing_issues(
            "https://github.com/Recidiviz/pulse-data/pull/100",
        )

        self.assertEqual(
            result,
            [
                LinearIssue(team_prefix="OBT", number=123),
                LinearIssue(team_prefix="ENG", number=789),
            ],
        )

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_returns_empty_when_no_closing_issues(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"data": {"attachmentsForURL": {"nodes": []}}},
        )

        client = LinearClient(FAKE_API_KEY)
        result = client.get_closing_issues(
            "https://github.com/Recidiviz/pulse-data/pull/100",
        )

        self.assertEqual(result, [])

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_raises_on_api_error(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=500,
            text="Internal Server Error",
        )

        client = LinearClient(FAKE_API_KEY)
        with self.assertRaises(LinearApiError):
            client.get_closing_issues(
                "https://github.com/Recidiviz/pulse-data/pull/100",
            )

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_raises_on_graphql_errors(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"errors": [{"message": "Not found"}]},
        )

        client = LinearClient(FAKE_API_KEY)
        with self.assertRaises(LinearApiError):
            client.get_closing_issues(
                "https://github.com/Recidiviz/pulse-data/pull/100",
            )

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_handles_metadata_as_string(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": {
                    "attachmentsForURL": {
                        "nodes": [
                            {
                                "metadata": '{"linkKind": "closes"}',
                                "issue": {"identifier": "OBT-123"},
                            },
                        ]
                    }
                }
            },
        )

        client = LinearClient(FAKE_API_KEY)
        result = client.get_closing_issues(
            "https://github.com/Recidiviz/pulse-data/pull/100",
        )

        self.assertEqual(result, [LinearIssue(team_prefix="OBT", number=123)])


class ResolveLinearToGithubTest(unittest.TestCase):
    """Tests for LinearClient.resolve_linear_to_github()."""

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_returns_github_issue(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": {
                    "issue": {
                        "attachments": {
                            "nodes": [
                                {
                                    "url": "https://github.com/Recidiviz/pulse-data/issues/45678"
                                }
                            ]
                        }
                    }
                }
            },
        )

        client = LinearClient(FAKE_API_KEY)
        result = client.resolve_linear_to_github(
            LinearIssue(team_prefix="OBT", number=123)
        )

        self.assertEqual(
            result,
            GithubIssue(repo="Recidiviz/pulse-data", number=45678),
        )

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_returns_none_when_no_github_attachment(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"data": {"issue": {"attachments": {"nodes": []}}}},
        )

        client = LinearClient(FAKE_API_KEY)
        result = client.resolve_linear_to_github(
            LinearIssue(team_prefix="OBT", number=123)
        )

        self.assertIsNone(result)


class ResolveGithubToLinearTest(unittest.TestCase):
    """Tests for LinearClient.resolve_github_to_linear()."""

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_returns_linear_issue(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": {
                    "attachmentsForURL": {
                        "nodes": [{"issue": {"identifier": "OBT-12345"}}]
                    }
                }
            },
        )

        client = LinearClient(FAKE_API_KEY)
        result = client.resolve_github_to_linear(
            GithubIssue(repo="Recidiviz/pulse-data", number=45678),
        )

        self.assertEqual(result, LinearIssue(team_prefix="OBT", number=12345))

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_returns_none_when_no_linear_issue(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"data": {"attachmentsForURL": {"nodes": []}}},
        )

        client = LinearClient(FAKE_API_KEY)
        result = client.resolve_github_to_linear(
            GithubIssue(repo="Recidiviz/pulse-data", number=45678),
        )

        self.assertIsNone(result)


FAKE_PR_URL = "https://github.com/Recidiviz/pulse-data/pull/100"


class GetAllPrAttachmentsTest(unittest.TestCase):
    """Tests for LinearClient.get_all_pr_attachments()."""

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_returns_all_attachments(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": {
                    "attachmentsForURL": {
                        "nodes": [
                            {
                                "id": "att-1",
                                "metadata": {
                                    "source": "auto-link-action",
                                    "linkKind": "closes",
                                },
                                "issue": {"identifier": "OBT-111"},
                            },
                            {
                                "id": "att-2",
                                "metadata": {"linkKind": "contributes"},
                                "issue": {"identifier": "OBT-222"},
                            },
                            {
                                "id": "att-3",
                                "metadata": {
                                    "source": "auto-link-action",
                                    "linkKind": "contributes",
                                },
                                "issue": {"identifier": "OBT-333"},
                            },
                        ]
                    }
                }
            },
        )

        client = LinearClient(FAKE_API_KEY)
        attachments = client.get_all_pr_attachments(FAKE_PR_URL)

        self.assertEqual(
            attachments,
            [
                LinearAttachment(
                    id="att-1",
                    issue_identifier="OBT-111",
                    link_kind=LinkKind.CLOSES,
                    source="auto-link-action",
                ),
                LinearAttachment(
                    id="att-2",
                    issue_identifier="OBT-222",
                    link_kind=LinkKind.CONTRIBUTES,
                    source=None,
                ),
                LinearAttachment(
                    id="att-3",
                    issue_identifier="OBT-333",
                    link_kind=LinkKind.CONTRIBUTES,
                    source="auto-link-action",
                ),
            ],
        )

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_returns_empty_when_no_attachments(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"data": {"attachmentsForURL": {"nodes": []}}},
        )

        client = LinearClient(FAKE_API_KEY)
        self.assertEqual(client.get_all_pr_attachments(FAKE_PR_URL), [])

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_handles_metadata_as_string(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": {
                    "attachmentsForURL": {
                        "nodes": [
                            {
                                "id": "att-1",
                                "metadata": '{"source": "my-source", "linkKind": "closes"}',
                                "issue": {"identifier": "OBT-111"},
                            },
                        ]
                    }
                }
            },
        )

        client = LinearClient(FAKE_API_KEY)
        attachments = client.get_all_pr_attachments(FAKE_PR_URL)

        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].link_kind, LinkKind.CLOSES)
        self.assertEqual(attachments[0].source, "my-source")


class CreatePrAttachmentTest(unittest.TestCase):
    """Tests for LinearClient.create_pr_attachment()."""

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_creates_attachment(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": {
                    "attachmentCreate": {
                        "success": True,
                        "attachment": {"id": "new-att-id"},
                    }
                }
            },
        )

        client = LinearClient(FAKE_API_KEY)
        result = client.create_pr_attachment(
            "OBT-123", FAKE_PR_URL, 100, LinkKind.CLOSES, "my-source"
        )

        self.assertEqual(result, "new-att-id")
        call_args = mock_post.call_args
        sent_body = call_args.kwargs.get("json") or call_args[1]["json"]
        variables = sent_body["variables"]
        self.assertEqual(variables["input"]["issueId"], "OBT-123")
        self.assertEqual(variables["input"]["url"], FAKE_PR_URL)
        self.assertEqual(variables["input"]["title"], "PR #100")
        self.assertEqual(
            variables["input"]["metadata"],
            {"source": "my-source", "linkKind": "closes"},
        )

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_raises_on_api_error(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=500, text="Internal Server Error"
        )

        client = LinearClient(FAKE_API_KEY)
        with self.assertRaises(LinearApiError):
            client.create_pr_attachment(
                "OBT-123", FAKE_PR_URL, 100, LinkKind.CLOSES, "my-source"
            )


class UpdateAttachmentTest(unittest.TestCase):
    """Tests for LinearClient.update_attachment()."""

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_updates_attachment(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"data": {"attachmentUpdate": {"success": True}}},
        )

        client = LinearClient(FAKE_API_KEY)
        client.update_attachment("att-1", "PR #42", LinkKind.CONTRIBUTES, "my-source")

        call_args = mock_post.call_args
        sent_body = call_args.kwargs.get("json") or call_args[1]["json"]
        variables = sent_body["variables"]
        self.assertEqual(variables["id"], "att-1")
        self.assertEqual(variables["input"]["title"], "PR #42")
        self.assertEqual(
            variables["input"]["metadata"],
            {"source": "my-source", "linkKind": "contributes"},
        )


class DeleteAttachmentTest(unittest.TestCase):
    """Tests for LinearClient.delete_attachment()."""

    @patch("recidiviz.repo.linear_client.requests.post")
    def test_deletes_attachment(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"data": {"attachmentDelete": {"success": True}}},
        )

        client = LinearClient(FAKE_API_KEY)
        client.delete_attachment("att-1")

        call_args = mock_post.call_args
        sent_body = call_args.kwargs.get("json") or call_args[1]["json"]
        self.assertEqual(sent_body["variables"]["id"], "att-1")
