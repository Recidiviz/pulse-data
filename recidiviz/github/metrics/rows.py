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
"""Maps one pull request node from the GitHub GraphQL API to rows for the seven
github_api_raw tables.

For example, a merged pull request with one approving review, one inline comment
and two changed files maps to rows like these (columns abridged):

    pull_requests:   pr_number=101, author_login=synthetic-user-1, state=MERGED,
                     review_count=1, changed_files=2, truncated_connections=[]
    reviews:         pr_number=101, review_id=PRR_1, state=APPROVED,
                     submitted_at=2026-10-02T17:00:00Z, comment_count=1
    review_comments: pr_number=101, comment_id=PRRC_1, review_id=PRR_1,
                     path=src/app.py, body_length=48
    files:        pr_number=101, path=src/app.py, additions=35
    files:        pr_number=101, path=src/app_test.py, additions=5

Every row also carries the crawl_id and crawled_at of the run, and a raw column
holding its own node as a dict, which the load job writes as a JSON object. No row
stores a review or comment body, only its length. Timestamps stay in the ISO 8601
form GitHub returns.
"""
from datetime import datetime
from typing import Any

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common import attr_validators
from recidiviz.github.metrics.queries import NESTED_CONNECTIONS, NestedConnection
from recidiviz.github.metrics.tables import (
    COMMITS_ADDRESS,
    FILES_ADDRESS,
    ISSUE_COMMENTS_ADDRESS,
    PULL_REQUESTS_ADDRESS,
    REVIEW_COMMENTS_ADDRESS,
    REVIEWS_ADDRESS,
    TIMELINE_ITEMS_ADDRESS,
)


@attr.define(frozen=True, kw_only=True)
class CrawlContext:
    """Identifies one crawl run. Every row the run writes carries both values."""

    crawl_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """A UUID made when the run starts."""

    crawled_at: datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    """When the run started, in UTC."""


def _login(account: dict[str, Any] | None) -> str | None:
    return account["login"] if account else None


def _type_name(account: dict[str, Any] | None) -> str | None:
    return account["__typename"] if account else None


def _without(node: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: value for key, value in node.items() if key not in keys}


def _connection_pages(
    node: dict[str, Any], connection: NestedConnection
) -> list[dict[str, Any]]:
    """Returns the first page of the connection on the pull request node, or of each
    review's connection when it is nested in reviews."""
    if connection.is_nested_in_reviews:
        return [review[connection.field] for review in node["reviews"]["nodes"]]
    return [node[connection.field]]


def truncated_connections(node: dict[str, Any]) -> list[NestedConnection]:
    """Returns the connections on the pull request node that have more pages to
    read, in NESTED_CONNECTIONS order. review_comments is included when any review
    has more comments."""
    return [
        connection
        for connection in NESTED_CONNECTIONS
        if any(
            page["pageInfo"]["hasNextPage"]
            for page in _connection_pages(node, connection)
        )
    ]


def merge_overflow(page: dict[str, Any], extra_nodes: list[dict[str, Any]]) -> None:
    """Appends nodes read by an overflow query to one connection, in place, and
    marks that connection as fully read.

    Args:
        page: The connection as it sits on the pull request node or on one of its
            reviews, for example node["files"] or review["comments"].
        extra_nodes: The nodes the overflow query returned after the ones the
            connection already holds.
    """
    page["nodes"].extend(extra_nodes)
    page["pageInfo"]["hasNextPage"] = False


def _pull_request_row(node: dict[str, Any]) -> dict[str, Any]:
    return {
        "pr_id": node["id"],
        "title": node["title"],
        "author_login": _login(node["author"]),
        "author_type": _type_name(node["author"]),
        "state": node["state"],
        "is_draft": node["isDraft"],
        "created_at": node["createdAt"],
        "updated_at": node["updatedAt"],
        "closed_at": node["closedAt"],
        "merged_at": node["mergedAt"],
        "merged_by_login": _login(node["mergedBy"]),
        "merge_commit_oid": node["mergeCommit"]["oid"] if node["mergeCommit"] else None,
        "base_ref_name": node["baseRefName"],
        "head_ref_name": node["headRefName"],
        "is_cross_repository": node["isCrossRepository"],
        "additions": node["additions"],
        "deletions": node["deletions"],
        "changed_files": node["changedFiles"],
        "labels": [label["name"] for label in node["labels"]["nodes"]],
        "review_count": node["reviews"]["totalCount"],
        "commit_count": node["commits"]["totalCount"],
        "timeline_count": node["timelineItems"]["totalCount"],
        "truncated_connections": [c.name for c in truncated_connections(node)],
        "raw": _without(
            node,
            *(c.field for c in NESTED_CONNECTIONS if not c.is_nested_in_reviews),
        ),
    }


def _review_row(review: dict[str, Any]) -> dict[str, Any]:
    return {
        "review_id": review["id"],
        "state": review["state"],
        "submitted_at": review["submittedAt"],
        "author_login": _login(review["author"]),
        "author_type": _type_name(review["author"]),
        "body_length": len(review["body"] or ""),
        "comment_count": review["comments"]["totalCount"],
        "raw": _without(review, "body", "comments"),
    }


def _review_comment_row(comment: dict[str, Any], review_id: str) -> dict[str, Any]:
    return {
        "comment_id": comment["id"],
        "review_id": review_id,
        "created_at": comment["createdAt"],
        "author_login": _login(comment["author"]),
        "author_type": _type_name(comment["author"]),
        "path": comment["path"],
        "body_length": len(comment["body"] or ""),
        "raw": _without(comment, "body"),
    }


def _issue_comment_row(comment: dict[str, Any]) -> dict[str, Any]:
    return {
        "comment_id": comment["id"],
        "created_at": comment["createdAt"],
        "author_login": _login(comment["author"]),
        "author_type": _type_name(comment["author"]),
        "body_length": len(comment["body"] or ""),
        "raw": _without(comment, "body"),
    }


def _timeline_item_row(item: dict[str, Any]) -> dict[str, Any]:
    # Only review request events select requestedReviewer, and only labeled and
    # unlabeled events select label. requestedReviewer holds login for a User or
    # Bot, slug for a Team, and neither for a Mannequin.
    requested_reviewer = item.get("requestedReviewer") or {}
    label = item.get("label") or {}
    return {
        "item_type": item["__typename"],
        "occurred_at": item["createdAt"],
        "actor_login": _login(item["actor"]),
        "requested_reviewer_login": requested_reviewer.get("login"),
        "requested_team_slug": requested_reviewer.get("slug"),
        "label_name": label.get("name"),
        "raw": item,
    }


def _commit_row(pull_request_commit: dict[str, Any]) -> dict[str, Any]:
    commit = pull_request_commit["commit"]
    # Commit.author is nullable, and its user is null when no GitHub account
    # matches the commit's author email.
    author = commit["author"]
    return {
        "commit_oid": commit["oid"],
        "authored_at": commit["authoredDate"],
        "committed_at": commit["committedDate"],
        "author_login": _login(author["user"]) if author else None,
        "additions": commit["additions"],
        "deletions": commit["deletions"],
        "raw": pull_request_commit,
    }


def _file_row(file: dict[str, Any]) -> dict[str, Any]:
    return {
        "path": file["path"],
        "additions": file["additions"],
        "deletions": file["deletions"],
        "change_type": file["changeType"],
        "raw": file,
    }


def rows_for_pull_request(
    node: dict[str, Any], repository_full_name: str, ctx: CrawlContext
) -> dict[BigQueryAddress, list[dict[str, Any]]]:
    """Returns the rows for one pull request node, keyed by the address of the
    table they belong in.

    Every one of the seven tables is present, with an empty list when the pull
    request has nothing for that table.
    """
    common = {
        "repository_full_name": repository_full_name,
        "pr_number": node["number"],
        "crawl_id": ctx.crawl_id,
        "crawled_at": ctx.crawled_at.isoformat(),
    }
    reviews = node["reviews"]["nodes"]
    table_rows: dict[BigQueryAddress, list[dict[str, Any]]] = {
        PULL_REQUESTS_ADDRESS: [_pull_request_row(node)],
        REVIEWS_ADDRESS: [_review_row(review) for review in reviews],
        REVIEW_COMMENTS_ADDRESS: [
            _review_comment_row(comment, review["id"])
            for review in reviews
            for comment in review["comments"]["nodes"]
        ],
        ISSUE_COMMENTS_ADDRESS: [
            _issue_comment_row(comment) for comment in node["comments"]["nodes"]
        ],
        TIMELINE_ITEMS_ADDRESS: [
            _timeline_item_row(item) for item in node["timelineItems"]["nodes"]
        ],
        COMMITS_ADDRESS: [_commit_row(commit) for commit in node["commits"]["nodes"]],
        FILES_ADDRESS: [_file_row(file) for file in node["files"]["nodes"]],
    }
    return {
        address: [{**common, **row} for row in rows]
        for address, rows in table_rows.items()
    }
