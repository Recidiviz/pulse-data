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
"""Tests for the eng_metrics.pr_first_feedback view."""
from datetime import datetime, timezone

from recidiviz.monitoring.eng_metrics.dataset_config import (
    GITHUB_EVENTS_ARCHIVE_ADDRESS,
)
from recidiviz.monitoring.eng_metrics.pr_first_feedback import (
    PR_FIRST_FEEDBACK_VIEW_BUILDER,
)
from recidiviz.source_tables.yaml_managed.collect_yaml_managed_source_table_configs import (
    build_source_table_repository_for_yaml_managed_tables,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

_REPO = "Recidiviz/pulse-data"
_SOURCE = "github_events_v2"


class PrFirstFeedbackViewTest(BigQueryEmulatorTestCase):
    """Tests for PR_FIRST_FEEDBACK_VIEW_BUILDER."""

    def setUp(self) -> None:
        super().setUp()
        repository = build_source_table_repository_for_yaml_managed_tables(
            project_id=None
        )
        self.create_mock_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            schema=repository.get_config(GITHUB_EVENTS_ARCHIVE_ADDRESS).schema_fields,
        )

    def _run_pickup_query_test(self, expected_result: list[dict]) -> None:
        view_query = PR_FIRST_FEEDBACK_VIEW_BUILDER.build().view_query
        self.run_query_test(
            query_str=f"""
SELECT
    pr_number,
    ready_at,
    ready_at_source,
    first_feedback_at,
    first_feedback_login,
    first_feedback_kind,
    pickup_hours
FROM ({view_query})
ORDER BY pr_number
""",
            expected_result=expected_result,
        )

    def test_pr_opened_ready_uses_created_at(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T12:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T12:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T12:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "opened_ready",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "bob",
                    "first_feedback_kind": "review",
                    "pickup_hours": 2.0,
                }
            ]
        )

    def test_draft_pr_uses_first_ready_for_review_event(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": True,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T13:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T13:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "ready_for_review",
                    "webhook_payload": {
                        "action": "ready_for_review",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d3",
                    "event_timestamp": "2026-10-01T14:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T14:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T14:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 10, 1, 13, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "ready_for_review_event",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 14, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "bob",
                    "first_feedback_kind": "review",
                    "pickup_hours": 1.0,
                }
            ]
        )

    def test_draft_pr_never_readied_has_null_ready_and_pickup(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": True,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T12:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T12:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "commented",
                            "submitted_at": "2026-10-01T12:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": None,
                    "ready_at_source": None,
                    "first_feedback_at": None,
                    "first_feedback_login": None,
                    "first_feedback_kind": None,
                    "pickup_hours": None,
                }
            ]
        )

    def test_first_ready_event_wins_when_readied_twice(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": True,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T13:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T13:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "ready_for_review",
                    "webhook_payload": {
                        "action": "ready_for_review",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d3",
                    "event_timestamp": "2026-10-01T14:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T14:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "converted_to_draft",
                    "webhook_payload": {
                        "action": "converted_to_draft",
                        "pull_request": {
                            "number": 1,
                            "draft": True,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d4",
                    "event_timestamp": "2026-10-01T15:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T15:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "ready_for_review",
                    "webhook_payload": {
                        "action": "ready_for_review",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d5",
                    "event_timestamp": "2026-10-01T16:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T16:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T16:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 10, 1, 13, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "ready_for_review_event",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 16, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "bob",
                    "first_feedback_kind": "review",
                    "pickup_hours": 3.0,
                }
            ]
        )

    def test_feedback_before_ready_is_ignored(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": True,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                # A comment while the PR is still a draft does not count.
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T11:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T11:00:00Z",
                    "github_event_type": "issue_comment",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "created",
                    "webhook_payload": {
                        "action": "created",
                        "comment": {
                            "created_at": "2026-10-01T11:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "issue": {
                            "number": 1,
                            "pull_request": {
                                "url": "https://api.github.com/repos/Recidiviz/pulse-data/pulls/1"
                            },
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d3",
                    "event_timestamp": "2026-10-01T13:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T13:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "ready_for_review",
                    "webhook_payload": {
                        "action": "ready_for_review",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d4",
                    "event_timestamp": "2026-10-01T14:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T14:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "carol",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T14:00:00Z",
                            "user": {"login": "carol", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 10, 1, 13, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "ready_for_review_event",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 14, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "carol",
                    "first_feedback_kind": "review",
                    "pickup_hours": 1.0,
                }
            ]
        )

    def test_feedback_at_ready_instant_counts(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "opened_ready",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "bob",
                    "first_feedback_kind": "review",
                    "pickup_hours": 0.0,
                }
            ]
        )

    def test_bot_typed_feedback_is_ignored(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                # Identical to the human review below except the account type, and
                # earlier, so it would win if bots counted.
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T10:05:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:05:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "commented",
                            "submitted_at": "2026-10-01T10:05:00Z",
                            "user": {"login": "bob", "type": "Bot"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d3",
                    "event_timestamp": "2026-10-01T12:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T12:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "commented",
                            "submitted_at": "2026-10-01T12:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "opened_ready",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "bob",
                    "first_feedback_kind": "review",
                    "pickup_hours": 2.0,
                }
            ]
        )

    def test_helperbot_feedback_is_ignored(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                # GitHub types helperbot-recidiviz as a User, so only the login
                # distinguishes it from the human review below.
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T10:05:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:05:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "helperbot-recidiviz",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T10:05:00Z",
                            "user": {
                                "login": "helperbot-recidiviz",
                                "type": "User",
                            },
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d3",
                    "event_timestamp": "2026-10-01T12:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T12:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T12:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "opened_ready",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "bob",
                    "first_feedback_kind": "review",
                    "pickup_hours": 2.0,
                }
            ]
        )

    def test_author_own_comment_is_ignored(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T10:30:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:30:00Z",
                    "github_event_type": "issue_comment",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "created",
                    "webhook_payload": {
                        "action": "created",
                        "comment": {
                            "created_at": "2026-10-01T10:30:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                        "issue": {
                            "number": 1,
                            "pull_request": {
                                "url": "https://api.github.com/repos/Recidiviz/pulse-data/pulls/1"
                            },
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d3",
                    "event_timestamp": "2026-10-01T12:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T12:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T12:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "opened_ready",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "bob",
                    "first_feedback_kind": "review",
                    "pickup_hours": 2.0,
                }
            ]
        )

    def test_author_exclusion_is_per_pr(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 2,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d3",
                    "event_timestamp": "2026-10-01T11:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T11:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T11:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d4",
                    "event_timestamp": "2026-10-01T12:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T12:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T12:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                        "pull_request": {"number": 2},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "opened_ready",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 11, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "bob",
                    "first_feedback_kind": "review",
                    "pickup_hours": 1.0,
                },
                {
                    "pr_number": 2,
                    "ready_at": datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "opened_ready",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "alice",
                    "first_feedback_kind": "review",
                    "pickup_hours": 2.0,
                },
            ]
        )

    def test_issue_comment_not_on_a_pr_is_ignored(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 7,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                # Issue number 7 is a plain issue, not the PR, so its payload has
                # no issue.pull_request key.
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T11:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T11:00:00Z",
                    "github_event_type": "issue_comment",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "created",
                    "webhook_payload": {
                        "action": "created",
                        "comment": {
                            "created_at": "2026-10-01T11:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "issue": {"number": 7},
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d3",
                    "event_timestamp": "2026-10-01T12:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T12:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "carol",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T12:00:00Z",
                            "user": {"login": "carol", "type": "User"},
                        },
                        "pull_request": {"number": 7},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 7,
                    "ready_at": datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "opened_ready",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "carol",
                    "first_feedback_kind": "review",
                    "pickup_hours": 2.0,
                }
            ]
        )

    def test_review_edited_and_dismissed_events_are_ignored(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                # The two ignored rows match the submitted review below in every
                # field but action, and are earlier, so they would win if counted.
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T11:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T11:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "edited",
                    "webhook_payload": {
                        "action": "edited",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T11:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d3",
                    "event_timestamp": "2026-10-01T11:30:00+00:00",
                    "event_timestamp_ts": "2026-10-01T11:30:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "dismissed",
                    "webhook_payload": {
                        "action": "dismissed",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T11:30:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d4",
                    "event_timestamp": "2026-10-01T12:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T12:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T12:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "opened_ready",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "bob",
                    "first_feedback_kind": "review",
                    "pickup_hours": 2.0,
                }
            ]
        )

    def test_review_comment_counts_as_feedback(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T11:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T11:00:00Z",
                    "github_event_type": "pull_request_review_comment",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "created",
                    "webhook_payload": {
                        "action": "created",
                        "comment": {
                            "created_at": "2026-10-01T11:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d3",
                    "event_timestamp": "2026-10-01T12:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T12:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "carol",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T12:00:00Z",
                            "user": {"login": "carol", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "opened_ready",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 11, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "bob",
                    "first_feedback_kind": "review_comment",
                    "pickup_hours": 1.0,
                }
            ]
        )

    def test_tie_at_same_instant_prefers_review_then_login(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T12:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T12:00:00Z",
                    "github_event_type": "pull_request_review_comment",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "created",
                    "webhook_payload": {
                        "action": "created",
                        "comment": {
                            "created_at": "2026-10-01T12:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d3",
                    "event_timestamp": "2026-10-01T12:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T12:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "carol",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "commented",
                            "submitted_at": "2026-10-01T12:00:00Z",
                            "user": {"login": "carol", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "opened_ready",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "carol",
                    "first_feedback_kind": "review",
                    "pickup_hours": 2.0,
                }
            ]
        )

    def test_repo_outside_allowlist_is_excluded(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": "Recidiviz/recidiviz-dashboards-peter-fork",
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 2,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        view_query = PR_FIRST_FEEDBACK_VIEW_BUILDER.build().view_query
        self.run_query_test(
            query_str=f"""
SELECT repository_full_name, pr_number
FROM ({view_query})
ORDER BY pr_number
""",
            expected_result=[{"repository_full_name": _REPO, "pr_number": 1}],
        )

    def test_bot_authored_pr_is_excluded(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "dependabot[bot]",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "dependabot[bot]", "type": "Bot"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 2,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        view_query = PR_FIRST_FEEDBACK_VIEW_BUILDER.build().view_query
        self.run_query_test(
            query_str=f"""
SELECT pr_number, pr_author_login
FROM ({view_query})
ORDER BY pr_number
""",
            expected_result=[{"pr_number": 2, "pr_author_login": "alice"}],
        )

    def test_pr_first_seen_on_synchronize_still_gets_a_row(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                # The archive has no opened event for this PR; the push at 11:00 is
                # the first event we have, and it still carries created_at.
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T11:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T11:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "synchronize",
                    "webhook_payload": {
                        "action": "synchronize",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T09:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T12:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T12:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T12:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 10, 1, 9, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "opened_ready",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "bob",
                    "first_feedback_kind": "review",
                    "pickup_hours": 3.0,
                }
            ]
        )

    def test_no_feedback_yet_yields_null_feedback_columns(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "opened_ready",
                    "first_feedback_at": None,
                    "first_feedback_login": None,
                    "first_feedback_kind": None,
                    "pickup_hours": None,
                }
            ]
        )

    def test_pr_first_seen_on_ready_for_review_uses_that_event(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                # The PR opened as a draft months earlier, before the archive
                # window; the ready_for_review event is the first one archived.
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-08-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-08-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "ready_for_review",
                    "webhook_payload": {
                        "action": "ready_for_review",
                        "pull_request": {
                            "number": 1,
                            "draft": False,
                            "created_at": "2026-03-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-08-02T10:00:00+00:00",
                    "event_timestamp_ts": "2026-08-02T10:00:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-08-02T10:00:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 1},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self._run_pickup_query_test(
            [
                {
                    "pr_number": 1,
                    "ready_at": datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "ready_for_review_event",
                    "first_feedback_at": datetime(
                        2026, 8, 2, 10, 0, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "bob",
                    "first_feedback_kind": "review",
                    "pickup_hours": 24.0,
                }
            ]
        )

    def test_full_row_for_a_reviewed_pr(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 42,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
                {
                    "delivery_id": "d2",
                    "event_timestamp": "2026-10-01T12:30:00+00:00",
                    "event_timestamp_ts": "2026-10-01T12:30:00Z",
                    "github_event_type": "pull_request_review",
                    "repository_full_name": _REPO,
                    "sender_login": "bob",
                    "action": "submitted",
                    "webhook_payload": {
                        "action": "submitted",
                        "review": {
                            "state": "approved",
                            "submitted_at": "2026-10-01T12:30:00Z",
                            "user": {"login": "bob", "type": "User"},
                        },
                        "pull_request": {"number": 42},
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        self.run_query_test(
            query_str=PR_FIRST_FEEDBACK_VIEW_BUILDER.build().view_query,
            expected_result=[
                {
                    "repository_full_name": "Recidiviz/pulse-data",
                    "pr_number": 42,
                    "pr_url": "https://github.com/Recidiviz/pulse-data/pull/42",
                    "pr_author_login": "alice",
                    "pr_created_at": datetime(
                        2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc
                    ),
                    "ready_at": datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
                    "ready_at_source": "opened_ready",
                    "first_feedback_at": datetime(
                        2026, 10, 1, 12, 30, 0, tzinfo=timezone.utc
                    ),
                    "first_feedback_login": "bob",
                    "first_feedback_kind": "review",
                    "pickup_hours": 2.5,
                }
            ],
        )

    def test_pr_url_is_built_from_repo_and_number(self) -> None:
        self.load_rows_into_table(
            address=GITHUB_EVENTS_ARCHIVE_ADDRESS,
            data=[
                {
                    "delivery_id": "d1",
                    "event_timestamp": "2026-10-01T10:00:00+00:00",
                    "event_timestamp_ts": "2026-10-01T10:00:00Z",
                    "github_event_type": "pull_request",
                    "repository_full_name": _REPO,
                    "sender_login": "alice",
                    "action": "opened",
                    "webhook_payload": {
                        "action": "opened",
                        "pull_request": {
                            "number": 42,
                            "draft": False,
                            "created_at": "2026-10-01T10:00:00Z",
                            "user": {"login": "alice", "type": "User"},
                        },
                    },
                    "siem_source_table": _SOURCE,
                },
            ],
        )
        view_query = PR_FIRST_FEEDBACK_VIEW_BUILDER.build().view_query
        self.run_query_test(
            query_str=f"SELECT pr_url FROM ({view_query})",
            expected_result=[
                {"pr_url": "https://github.com/Recidiviz/pulse-data/pull/42"}
            ],
        )
