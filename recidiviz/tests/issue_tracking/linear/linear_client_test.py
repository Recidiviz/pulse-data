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
from datetime import datetime, timezone

import requests
from mock import MagicMock, patch

from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.linear.linear_client import (
    LINEAR_API_MAX_ATTEMPTS,
    LinearApiError,
    LinearAttachment,
    LinearClient,
    LinearIssueInfo,
    RetryableLinearApiError,
)
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue
from recidiviz.issue_tracking.linear.linear_types import LinkKind

FAKE_API_KEY = "lin_api_test_key"

_original_query_sleep = LinearClient.query.retry.sleep  # type: ignore[attr-defined]


def setUpModule() -> None:
    # query retries transient failures with exponential backoff; no-op the
    # sleep so tests that exercise retries don't actually wait through it.
    LinearClient.query.retry.sleep = lambda *_args, **_kwargs: None  # type: ignore[attr-defined]


def tearDownModule() -> None:
    LinearClient.query.retry.sleep = _original_query_sleep  # type: ignore[attr-defined]


class GetClosingIssuesTest(unittest.TestCase):
    """Tests for LinearClient.get_closing_issues()."""

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_returns_closing_issues(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": {
                    "attachmentsForURL": {
                        "nodes": [
                            {
                                "id": "att-1",
                                "metadata": {"linkKind": "closes"},
                                "issue": {"identifier": "OBT-123"},
                            },
                            {
                                "id": "att-2",
                                "metadata": {"linkKind": "contributes"},
                                "issue": {"identifier": "OBT-456"},
                            },
                            {
                                "id": "att-3",
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

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
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

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_skips_attachment_with_no_link_kind(self, mock_post: MagicMock) -> None:
        # attachmentsForURL returns attachments created outside Linear's GitHub
        # integration, which carry no linkKind. These must be skipped rather
        # than crashing on LinkKind(None).
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": {
                    "attachmentsForURL": {
                        "nodes": [
                            {
                                "id": "att-1",
                                "metadata": {},
                                "issue": {"identifier": "OBT-123"},
                            },
                            {
                                "id": "att-2",
                                "metadata": {"linkKind": "closes"},
                                "issue": {"identifier": "OBT-456"},
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

        self.assertEqual(result, [LinearIssue(team_prefix="OBT", number=456)])

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
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

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
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


class ResolveLinearToGithubTest(unittest.TestCase):
    """Tests for LinearClient.resolve_linear_to_github()."""

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
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

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
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

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
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

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
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

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
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

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_attachment_with_no_link_kind_has_none_link_kind(
        self, mock_post: MagicMock
    ) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": {
                    "attachmentsForURL": {
                        "nodes": [
                            {
                                "id": "att-1",
                                "metadata": {},
                                "issue": {"identifier": "OBT-111"},
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
                    link_kind=None,
                    source=None,
                ),
            ],
        )

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_returns_empty_when_no_attachments(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"data": {"attachmentsForURL": {"nodes": []}}},
        )

        client = LinearClient(FAKE_API_KEY)
        self.assertEqual(client.get_all_pr_attachments(FAKE_PR_URL), [])


class CreatePrAttachmentTest(unittest.TestCase):
    """Tests for LinearClient.create_pr_attachment()."""

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
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

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
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

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
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

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
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


class GetRecentlyClosedIssuesTest(unittest.TestCase):
    """Tests for LinearClient.get_recently_closed_issues()."""

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_returns_closed_issues(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": {
                    "issues": {
                        "nodes": [
                            {
                                "id": "uuid-1",
                                "identifier": "OBT-100",
                                "title": "Fix widget",
                                "team": {"key": "OBT"},
                            },
                            {
                                "id": "uuid-2",
                                "identifier": "ENG-200",
                                "title": "Refactor",
                                "team": {"key": "ENG"},
                            },
                        ],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            },
        )

        client = LinearClient(FAKE_API_KEY)
        result = client.get_recently_closed_issues(
            datetime(2026, 1, 1, tzinfo=timezone.utc), exclude_with_labels=[]
        )

        self.assertEqual(
            result,
            [
                LinearIssueInfo(
                    linear_issue=LinearIssue(team_prefix="OBT", number=100),
                    uuid="uuid-1",
                    title="Fix widget",
                ),
                LinearIssueInfo(
                    linear_issue=LinearIssue(team_prefix="ENG", number=200),
                    uuid="uuid-2",
                    title="Refactor",
                ),
            ],
        )

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_query_matches_completed_or_canceled(self, mock_post: MagicMock) -> None:
        # Canceled issues set canceledAt rather than completedAt, so the query
        # must match either timestamp falling within the window. Both spellings
        # also matter: Linear's state type enum uses "canceled" (one l).
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": {
                    "issues": {
                        "nodes": [],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            },
        )

        client = LinearClient(FAKE_API_KEY)
        client.get_recently_closed_issues(
            datetime(2026, 1, 1, tzinfo=timezone.utc), exclude_with_labels=[]
        )

        sent_query = (
            mock_post.call_args.kwargs.get("json") or mock_post.call_args[1]["json"]
        )["query"]
        self.assertIn('type: { in: ["completed", "canceled"] }', sent_query)
        self.assertIn("completedAt: { gte: $since }", sent_query)
        self.assertIn("canceledAt: { gte: $since }", sent_query)

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_excludes_labels_server_side(self, mock_post: MagicMock) -> None:
        # The label exclusion is pushed into the query rather than filtered
        # client-side, so the filter clause and the variable must both be sent.
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": {
                    "issues": {
                        "nodes": [],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            },
        )

        client = LinearClient(FAKE_API_KEY)
        client.get_recently_closed_issues(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            exclude_with_labels=["Stale Raw Data", "Dataflow Pipeline Failure"],
        )

        sent_body = (
            mock_post.call_args.kwargs.get("json") or mock_post.call_args[1]["json"]
        )
        self.assertIn(
            "labels: { every: { name: { nin: $excludeLabels } } }",
            sent_body["query"],
        )
        self.assertEqual(
            sent_body["variables"]["excludeLabels"],
            ["Stale Raw Data", "Dataflow Pipeline Failure"],
        )

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_handles_pagination(self, mock_post: MagicMock) -> None:
        mock_post.side_effect = [
            MagicMock(
                status_code=200,
                json=lambda: {
                    "data": {
                        "issues": {
                            "nodes": [
                                {
                                    "id": "uuid-1",
                                    "identifier": "OBT-100",
                                    "title": "First",
                                    "team": {"key": "OBT"},
                                },
                            ],
                            "pageInfo": {
                                "hasNextPage": True,
                                "endCursor": "cursor-1",
                            },
                        }
                    }
                },
            ),
            MagicMock(
                status_code=200,
                json=lambda: {
                    "data": {
                        "issues": {
                            "nodes": [
                                {
                                    "id": "uuid-2",
                                    "identifier": "ENG-200",
                                    "title": "Second",
                                    "team": {"key": "ENG"},
                                },
                            ],
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        }
                    }
                },
            ),
        ]

        client = LinearClient(FAKE_API_KEY)
        result = client.get_recently_closed_issues(
            datetime(2026, 1, 1, tzinfo=timezone.utc), exclude_with_labels=[]
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].identifier, "OBT-100")
        self.assertEqual(result[1].identifier, "ENG-200")
        self.assertEqual(mock_post.call_count, 2)
        second_call_body = (
            mock_post.call_args_list[1].kwargs.get("json")
            or mock_post.call_args_list[1][1]["json"]
        )
        self.assertEqual(second_call_body["variables"]["after"], "cursor-1")

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_returns_empty_for_no_results(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": {
                    "issues": {
                        "nodes": [],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            },
        )

        client = LinearClient(FAKE_API_KEY)
        result = client.get_recently_closed_issues(
            datetime(2026, 1, 1, tzinfo=timezone.utc), exclude_with_labels=[]
        )
        self.assertEqual(result, [])

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_raises_on_api_error(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=500, text="Internal Server Error"
        )

        client = LinearClient(FAKE_API_KEY)
        with self.assertRaises(LinearApiError):
            client.get_recently_closed_issues(
                datetime(2026, 1, 1, tzinfo=timezone.utc), exclude_with_labels=[]
            )


class QueryRetryTest(unittest.TestCase):
    """Tests for the retry behavior of LinearClient.query()."""

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_retries_transient_5xx_then_succeeds(self, mock_post: MagicMock) -> None:
        mock_post.side_effect = [
            MagicMock(status_code=502, text="Bad gateway"),
            MagicMock(status_code=200, json=lambda: {"data": {"ok": True}}),
        ]

        client = LinearClient(FAKE_API_KEY)
        result = client.query("query { ok }", {})

        self.assertEqual(result, {"ok": True})
        self.assertEqual(mock_post.call_count, 2)

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_retries_network_error_then_succeeds(self, mock_post: MagicMock) -> None:
        mock_post.side_effect = [
            requests.ConnectionError("connection reset"),
            MagicMock(status_code=200, json=lambda: {"data": {"ok": True}}),
        ]

        client = LinearClient(FAKE_API_KEY)
        result = client.query("query { ok }", {})

        self.assertEqual(result, {"ok": True})
        self.assertEqual(mock_post.call_count, 2)

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_gives_up_after_max_attempts(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(status_code=502, text="Bad gateway")

        client = LinearClient(FAKE_API_KEY)
        with self.assertRaises(RetryableLinearApiError):
            client.query("query { ok }", {})

        self.assertEqual(mock_post.call_count, LINEAR_API_MAX_ATTEMPTS)

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_does_not_retry_client_error(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(status_code=401, text="Unauthorized")

        client = LinearClient(FAKE_API_KEY)
        with self.assertRaises(LinearApiError) as cm:
            client.query("query { ok }", {})

        self.assertNotIsInstance(cm.exception, RetryableLinearApiError)
        self.assertEqual(mock_post.call_count, 1)

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_does_not_retry_graphql_errors(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200, json=lambda: {"errors": [{"message": "Not found"}]}
        )

        client = LinearClient(FAKE_API_KEY)
        with self.assertRaises(LinearApiError):
            client.query("query { ok }", {})

        self.assertEqual(mock_post.call_count, 1)


FAKE_ISSUE_INFO = LinearIssueInfo(
    linear_issue=LinearIssue(team_prefix="OBT", number=100),
    uuid="uuid-obt-100",
    title="Fix widget",
)


def _mock_team_states_response(states: list[dict[str, object]]) -> MagicMock:
    # Real WorkflowState nodes carry a `position`; default one per index so
    # callers that don't care about ordering don't have to specify it.
    states_with_position = [
        {"position": float(i), **state} for i, state in enumerate(states)
    ]
    return MagicMock(
        status_code=200,
        json=lambda: {
            "data": {"teams": {"nodes": [{"states": {"nodes": states_with_position}}]}}
        },
    )


class ReopenIssueTest(unittest.TestCase):
    """Tests for LinearClient.reopen_issue()."""

    def setUp(self) -> None:
        linear_client_module = "recidiviz.issue_tracking.linear.linear_client"
        patcher = patch.dict(f"{linear_client_module}._state_id_cache", clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_looks_up_triage_state_and_updates_issue(
        self, mock_post: MagicMock
    ) -> None:
        mock_post.side_effect = [
            _mock_team_states_response(
                [
                    {"id": "s-started", "type": "started"},
                    {"id": "s-triage", "type": "triage"},
                ]
            ),
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"issueUpdate": {"success": True}}},
            ),
        ]

        client = LinearClient(FAKE_API_KEY)
        client.reopen_issue(FAKE_ISSUE_INFO)

        update_body = (
            mock_post.call_args_list[1].kwargs.get("json")
            or mock_post.call_args_list[1][1]["json"]
        )
        self.assertEqual(update_body["variables"]["id"], "uuid-obt-100")
        self.assertEqual(update_body["variables"]["input"], {"stateId": "s-triage"})

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_caches_triage_state_across_calls(self, mock_post: MagicMock) -> None:
        mock_post.side_effect = [
            _mock_team_states_response([{"id": "s-triage", "type": "triage"}]),
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"issueUpdate": {"success": True}}},
            ),
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"issueUpdate": {"success": True}}},
            ),
        ]

        second_issue = LinearIssueInfo(
            linear_issue=LinearIssue(team_prefix="OBT", number=200),
            uuid="uuid-obt-200",
            title="Another issue",
        )

        client = LinearClient(FAKE_API_KEY)
        client.reopen_issue(FAKE_ISSUE_INFO)
        client.reopen_issue(second_issue)

        # 1 team-states query + 2 issueUpdate mutations = 3 total
        self.assertEqual(mock_post.call_count, 3)

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_raises_when_no_suitable_state(self, mock_post: MagicMock) -> None:
        mock_post.return_value = _mock_team_states_response(
            [
                {"id": "s-started", "type": "started"},
                {"id": "s-done", "type": "completed"},
            ]
        )

        client = LinearClient(FAKE_API_KEY)
        with self.assertRaises(LinearApiError):
            client.reopen_issue(FAKE_ISSUE_INFO)

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_raises_on_api_error(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=500, text="Internal Server Error"
        )

        client = LinearClient(FAKE_API_KEY)
        with self.assertRaises(LinearApiError):
            client.reopen_issue(FAKE_ISSUE_INFO)


FAKE_LINEAR_ISSUE = LinearIssue(team_prefix="OBT", number=100)


def _mock_issue_state_response(uuid: str, state_type: str) -> MagicMock:
    return MagicMock(
        status_code=200,
        json=lambda: {"data": {"issue": {"id": uuid, "state": {"type": state_type}}}},
    )


class StartIssueIfUnstartedTest(unittest.TestCase):
    """Tests for LinearClient.start_issue_if_unstarted()."""

    def setUp(self) -> None:
        linear_client_module = "recidiviz.issue_tracking.linear.linear_client"
        patcher = patch.dict(f"{linear_client_module}._state_id_cache", clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_transitions_unstarted_issue_to_started(self, mock_post: MagicMock) -> None:
        mock_post.side_effect = [
            _mock_issue_state_response("uuid-obt-100", "unstarted"),
            _mock_team_states_response(
                [
                    {"id": "s-triage", "type": "triage"},
                    {"id": "s-started", "type": "started"},
                ]
            ),
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"issueUpdate": {"success": True}}},
            ),
        ]

        client = LinearClient(FAKE_API_KEY)
        self.assertTrue(client.start_issue_if_unstarted(FAKE_LINEAR_ISSUE))

        update_body = (
            mock_post.call_args_list[2].kwargs.get("json")
            or mock_post.call_args_list[2][1]["json"]
        )
        self.assertEqual(update_body["variables"]["id"], "uuid-obt-100")
        self.assertEqual(update_body["variables"]["input"], {"stateId": "s-started"})

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_picks_lowest_position_when_multiple_started_states(
        self, mock_post: MagicMock
    ) -> None:
        # OBT has two started states ("In Progress" and "In Review"); we must
        # transition to the lower-position one ("In Progress"), not whichever
        # the API happens to list first.
        mock_post.side_effect = [
            _mock_issue_state_response("uuid-obt-100", "unstarted"),
            _mock_team_states_response(
                [
                    {"id": "s-in-review", "type": "started", "position": 2.0},
                    {"id": "s-in-progress", "type": "started", "position": 1.0},
                ]
            ),
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"issueUpdate": {"success": True}}},
            ),
        ]

        client = LinearClient(FAKE_API_KEY)
        self.assertTrue(client.start_issue_if_unstarted(FAKE_LINEAR_ISSUE))

        update_body = (
            mock_post.call_args_list[2].kwargs.get("json")
            or mock_post.call_args_list[2][1]["json"]
        )
        self.assertEqual(
            update_body["variables"]["input"], {"stateId": "s-in-progress"}
        )

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_leaves_started_issue_untouched(self, mock_post: MagicMock) -> None:
        mock_post.return_value = _mock_issue_state_response("uuid-obt-100", "started")

        client = LinearClient(FAKE_API_KEY)
        self.assertFalse(client.start_issue_if_unstarted(FAKE_LINEAR_ISSUE))

        # Only the state-lookup query runs; no team-states lookup or issueUpdate.
        self.assertEqual(mock_post.call_count, 1)

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_leaves_completed_issue_untouched(self, mock_post: MagicMock) -> None:
        mock_post.return_value = _mock_issue_state_response("uuid-obt-100", "completed")

        client = LinearClient(FAKE_API_KEY)
        self.assertFalse(client.start_issue_if_unstarted(FAKE_LINEAR_ISSUE))

        self.assertEqual(mock_post.call_count, 1)

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_caches_started_state_across_calls(self, mock_post: MagicMock) -> None:
        mock_post.side_effect = [
            _mock_issue_state_response("uuid-obt-100", "unstarted"),
            _mock_team_states_response([{"id": "s-started", "type": "started"}]),
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"issueUpdate": {"success": True}}},
            ),
            _mock_issue_state_response("uuid-obt-200", "unstarted"),
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"issueUpdate": {"success": True}}},
            ),
        ]

        client = LinearClient(FAKE_API_KEY)
        client.start_issue_if_unstarted(FAKE_LINEAR_ISSUE)
        client.start_issue_if_unstarted(LinearIssue(team_prefix="OBT", number=200))

        # 1st call: state query + team-states query + issueUpdate = 3.
        # 2nd call: state query + issueUpdate = 2 (team-states cached). Total 5.
        self.assertEqual(mock_post.call_count, 5)

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_raises_on_api_error(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=500, text="Internal Server Error"
        )

        client = LinearClient(FAKE_API_KEY)
        with self.assertRaises(LinearApiError):
            client.start_issue_if_unstarted(FAKE_LINEAR_ISSUE)


FAKE_COMMENT_ISSUE_INFO = LinearIssueInfo(
    linear_issue=LinearIssue(team_prefix="OBT", number=123),
    uuid="uuid-123",
    title="Comment test issue",
)


class CreateCommentTest(unittest.TestCase):
    """Tests for LinearClient.create_comment()."""

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_creates_comment(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"data": {"commentCreate": {"success": True}}},
        )

        client = LinearClient(FAKE_API_KEY)
        client.create_comment(FAKE_COMMENT_ISSUE_INFO, "Hello world")

        call_args = mock_post.call_args
        sent_body = call_args.kwargs.get("json") or call_args[1]["json"]
        self.assertEqual(
            sent_body["variables"]["input"],
            {"issueId": "uuid-123", "body": "Hello world"},
        )

    @patch("recidiviz.issue_tracking.linear.linear_client.requests.post")
    def test_raises_on_api_error(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=500, text="Internal Server Error"
        )

        client = LinearClient(FAKE_API_KEY)
        with self.assertRaises(LinearApiError):
            client.create_comment(FAKE_COMMENT_ISSUE_INFO, "Hello world")
