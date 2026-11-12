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
"""Tests for mapping GraphQL pull request nodes to github_api_raw rows."""
import json
import os
from datetime import datetime, timezone
from typing import Any

import attr
import pytest

from recidiviz.github.metrics.queries import NESTED_CONNECTIONS, NestedConnection
from recidiviz.github.metrics.rows import (
    CrawlContext,
    merge_overflow,
    rows_for_pull_request,
    truncated_connections,
)
from recidiviz.github.metrics.tables import (
    ALL_TABLE_ADDRESSES,
    COMMITS_ADDRESS,
    FILES_ADDRESS,
    ISSUE_COMMENTS_ADDRESS,
    PULL_REQUESTS_ADDRESS,
    REVIEW_COMMENTS_ADDRESS,
    REVIEWS_ADDRESS,
    TIMELINE_ITEMS_ADDRESS,
)
from recidiviz.source_tables import yaml_managed
from recidiviz.source_tables.source_table_config import SourceTableConfig

_REPO = "Recidiviz/synthetic-repo"
_CTX = CrawlContext(
    crawl_id="00000000-0000-0000-0000-000000000001",
    crawled_at=datetime(2026, 11, 5, 3, 0, 0, tzinfo=timezone.utc),
)


def _fixture() -> dict[str, Any]:
    path = os.path.join(
        os.path.dirname(__file__), "fixtures", "synthetic_pull_request.json"
    )
    with open(path, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(name="node")
def node_fixture() -> dict[str, Any]:
    return _fixture()


def _connection(name: str) -> NestedConnection:
    [connection] = [c for c in NESTED_CONNECTIONS if c.name == name]
    return connection


def _schema_columns(table_id: str) -> set[str]:
    config = SourceTableConfig.from_file(
        os.path.join(
            os.path.dirname(yaml_managed.__file__), "github_api_raw", f"{table_id}.yaml"
        )
    )
    return {field.name for field in config.schema_fields}


def test_rows_for_pull_request_returns_every_table(node: dict[str, Any]) -> None:
    rows = rows_for_pull_request(node, _REPO, _CTX)

    assert {address: len(table_rows) for address, table_rows in rows.items()} == {
        PULL_REQUESTS_ADDRESS: 1,
        REVIEWS_ADDRESS: 2,
        REVIEW_COMMENTS_ADDRESS: 1,
        ISSUE_COMMENTS_ADDRESS: 1,
        TIMELINE_ITEMS_ADDRESS: 4,
        COMMITS_ADDRESS: 2,
        FILES_ADDRESS: 2,
    }


def test_rows_for_pull_request_keeps_every_table_when_empty(
    node: dict[str, Any],
) -> None:
    for connection in [
        "labels",
        "commits",
        "reviews",
        "comments",
        "files",
        "timelineItems",
    ]:
        node[connection]["nodes"] = []

    rows = rows_for_pull_request(node, _REPO, _CTX)

    assert len(rows) == 7
    assert len(rows[PULL_REQUESTS_ADDRESS]) == 1
    assert all(
        not table_rows
        for address, table_rows in rows.items()
        if address != PULL_REQUESTS_ADDRESS
    )


def test_pull_request_row_common_columns_and_raw_stripped(node: dict[str, Any]) -> None:
    row = rows_for_pull_request(node, _REPO, _CTX)[PULL_REQUESTS_ADDRESS][0]

    assert row["repository_full_name"] == _REPO
    assert row["pr_number"] == 101
    assert row["crawl_id"] == "00000000-0000-0000-0000-000000000001"
    assert row["crawled_at"] == "2026-11-05T03:00:00+00:00"
    raw = row["raw"]
    assert raw["title"] == "Synthetic PR 1"
    for connection in [
        "reviews",
        "comments",
        "commits",
        "files",
        "timelineItems",
        "labels",
    ]:
        assert connection not in raw
    assert row["truncated_connections"] == ["files"]


def test_pull_request_row_typed_columns(node: dict[str, Any]) -> None:
    row = rows_for_pull_request(node, _REPO, _CTX)[PULL_REQUESTS_ADDRESS][0]

    assert row == {
        **row,
        "pr_id": "PR_synthetic_101",
        "title": "Synthetic PR 1",
        "author_login": "synthetic-user-1",
        "author_type": "User",
        "state": "MERGED",
        "is_draft": False,
        "created_at": "2026-10-01T15:00:00Z",
        "merged_at": "2026-10-03T18:00:00Z",
        "merged_by_login": "synthetic-user-2",
        "merge_commit_oid": "1111111111111111111111111111111111111111",
        "base_ref_name": "main",
        "is_cross_repository": False,
        "additions": 40,
        "deletions": 12,
        "changed_files": 3,
        "labels": ["synthetic-label"],
        "review_count": 2,
        "commit_count": 2,
        "timeline_count": 4,
    }


def test_every_row_carries_common_columns(node: dict[str, Any]) -> None:
    for table_rows in rows_for_pull_request(node, _REPO, _CTX).values():
        for row in table_rows:
            assert row["repository_full_name"] == _REPO
            assert row["pr_number"] == 101
            assert row["crawl_id"] == _CTX.crawl_id
            assert row["crawled_at"] == "2026-11-05T03:00:00+00:00"
            assert isinstance(row["raw"], dict)
            # The load job serializes the whole row, so raw must reach BigQuery as
            # a JSON object, not as a string holding JSON text.
            assert isinstance(json.loads(json.dumps(row))["raw"], dict)


def test_null_author_maps_to_none(node: dict[str, Any]) -> None:
    node["author"] = None
    node["mergedBy"] = None
    node["mergeCommit"] = None

    row = rows_for_pull_request(node, _REPO, _CTX)[PULL_REQUESTS_ADDRESS][0]

    assert row["author_login"] is None
    assert row["author_type"] is None
    assert row["merged_by_login"] is None
    assert row["merge_commit_oid"] is None


def test_pending_review_keeps_null_submitted_at(node: dict[str, Any]) -> None:
    reviews = rows_for_pull_request(node, _REPO, _CTX)[REVIEWS_ADDRESS]

    pending = [r for r in reviews if r["state"] == "PENDING"]
    assert len(pending) == 1
    assert pending[0]["submitted_at"] is None
    assert pending[0]["author_login"] == "synthetic-user-3"
    approved = [r for r in reviews if r["state"] == "APPROVED"][0]
    assert approved["comment_count"] == 1
    assert "comments" not in approved["raw"]


def test_body_length_not_body(node: dict[str, Any]) -> None:
    rows = rows_for_pull_request(node, _REPO, _CTX)

    for table_rows in rows.values():
        for row in table_rows:
            assert "body" not in row
            assert "body" not in row["raw"]
    review_comment = rows[REVIEW_COMMENTS_ADDRESS][0]
    assert review_comment["body_length"] == len("Synthetic review comment.")
    assert review_comment["review_id"] == "PRR_synthetic_1"
    assert review_comment["path"] == "src/synthetic.py"
    assert rows[ISSUE_COMMENTS_ADDRESS][0]["body_length"] == len(
        "Synthetic bot comment."
    )
    assert rows[ISSUE_COMMENTS_ADDRESS][0]["author_type"] == "Bot"
    assert rows[ISSUE_COMMENTS_ADDRESS][0]["author_login"] == "synthetic-bot-1[bot]"


def test_timeline_rows_map_each_type(node: dict[str, Any]) -> None:
    by_type = {
        row["item_type"]: row
        for row in rows_for_pull_request(node, _REPO, _CTX)[TIMELINE_ITEMS_ADDRESS]
    }

    assert by_type["ReadyForReviewEvent"]["occurred_at"] == "2026-10-01T15:10:00Z"
    assert by_type["ReadyForReviewEvent"]["actor_login"] == "synthetic-user-1"
    assert by_type["ReviewRequestedEvent"]["requested_team_slug"] == "synthetic-team"
    assert by_type["ReviewRequestedEvent"]["requested_reviewer_login"] is None
    assert by_type["LabeledEvent"]["label_name"] == "synthetic-label"
    assert by_type["MergedEvent"]["label_name"] is None


def test_timeline_review_request_for_user(node: dict[str, Any]) -> None:
    node["timelineItems"]["nodes"] = [
        {
            "__typename": "ReviewRequestedEvent",
            "createdAt": "2026-10-01T15:11:00Z",
            "actor": None,
            "requestedReviewer": {"login": "synthetic-user-2"},
        }
    ]

    row = rows_for_pull_request(node, _REPO, _CTX)[TIMELINE_ITEMS_ADDRESS][0]

    assert row["requested_reviewer_login"] == "synthetic-user-2"
    assert row["requested_team_slug"] is None
    assert row["actor_login"] is None


def test_commit_rows(node: dict[str, Any]) -> None:
    commits = rows_for_pull_request(node, _REPO, _CTX)[COMMITS_ADDRESS]

    assert [c["author_login"] for c in commits] == ["synthetic-user-1", None]
    assert commits[0]["commit_oid"] == "2222222222222222222222222222222222222222"
    assert commits[0]["authored_at"] == "2026-09-30T20:00:00Z"
    assert commits[0]["committed_at"] == "2026-10-01T14:55:00Z"


def test_row_keys_match_yaml_schema(node: dict[str, Any]) -> None:
    rows = rows_for_pull_request(node, _REPO, _CTX)

    assert set(rows) == set(ALL_TABLE_ADDRESSES)
    for address, table_rows in rows.items():
        for row in table_rows:
            assert set(row.keys()) == _schema_columns(address.table_id)


def test_truncated_connections_finds_nested_review_comments(
    node: dict[str, Any],
) -> None:
    node["files"]["pageInfo"]["hasNextPage"] = False
    node["reviews"]["nodes"][0]["comments"]["pageInfo"]["hasNextPage"] = True
    node["timelineItems"]["pageInfo"]["hasNextPage"] = True

    assert [c.name for c in truncated_connections(node)] == [
        "review_comments",
        "timeline_items",
    ]


def test_merge_overflow_appends_and_clears_flag(node: dict[str, Any]) -> None:
    extra = [
        {
            "path": "src/synthetic_extra.py",
            "additions": 1,
            "deletions": 0,
            "changeType": "ADDED",
        }
    ]

    merge_overflow(node["files"], extra)

    assert [f["path"] for f in node["files"]["nodes"]] == [
        "src/synthetic.py",
        "src/synthetic_test.py",
        "src/synthetic_extra.py",
    ]
    assert node["files"]["pageInfo"]["hasNextPage"] is False
    assert truncated_connections(node) == []
    assert len(rows_for_pull_request(node, _REPO, _CTX)[FILES_ADDRESS]) == 3


def test_merge_overflow_for_review_comments(node: dict[str, Any]) -> None:
    review = node["reviews"]["nodes"][0]
    review["comments"]["pageInfo"]["hasNextPage"] = True
    extra = [
        {
            "id": "PRRC_synthetic_2",
            "createdAt": "2026-10-02T16:56:00Z",
            "path": "src/synthetic.py",
            "body": "Another synthetic comment.",
            "author": {"login": "synthetic-user-2", "__typename": "User"},
        }
    ]

    merge_overflow(review["comments"], extra)

    assert [c["id"] for c in review["comments"]["nodes"]] == [
        "PRRC_synthetic_1",
        "PRRC_synthetic_2",
    ]
    assert review["comments"]["pageInfo"]["hasNextPage"] is False
    assert _connection("review_comments") not in truncated_connections(node)


def test_crawl_context_requires_an_id_and_a_utc_start_time() -> None:
    with pytest.raises(ValueError):
        CrawlContext(crawl_id="", crawled_at=_CTX.crawled_at)
    with pytest.raises(ValueError):
        CrawlContext(crawl_id=_CTX.crawl_id, crawled_at=datetime(2026, 11, 5, 3, 0, 0))
    with pytest.raises(attr.exceptions.FrozenInstanceError):
        _CTX.crawl_id = "00000000-0000-0000-0000-000000000002"  # type: ignore[misc]
