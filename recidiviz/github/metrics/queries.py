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
"""GraphQL queries for the pull request crawl.

PR_SEARCH_QUERY reads pull requests 20 at a time with the first page of each
nested connection. When a nested connection has more pages, the crawl reads the
rest of it for that one pull request with its overflow_query in
NESTED_CONNECTIONS, starting from the nested connection's end cursor. The nested
page sizes fit most pull requests, and keep a search page near 3 points of the
hourly budget. GitHub charges about one point per 100 connection requests that a
query could make, and nested first values multiply.
"""
import attr

from recidiviz.common import attr_validators

_RATE_LIMIT = "rateLimit { cost remaining resetAt }"

_PAGE_INFO = "pageInfo { hasNextPage endCursor }"

_AUTHOR = "author { login __typename }"

_TIMELINE_ITEM_TYPES = (
    "READY_FOR_REVIEW_EVENT, CONVERT_TO_DRAFT_EVENT, REVIEW_REQUESTED_EVENT, "
    "REVIEW_REQUEST_REMOVED_EVENT, CLOSED_EVENT, REOPENED_EVENT, MERGED_EVENT, "
    "LABELED_EVENT, UNLABELED_EVENT, AUTO_MERGE_ENABLED_EVENT, "
    "AUTO_MERGE_DISABLED_EVENT"
)

_REQUESTED_REVIEWER = (
    "requestedReviewer { "
    "... on User { login } ... on Bot { login } ... on Team { slug } }"
)

# Selection sets for one node of each nested connection. The search query and
# the overflow queries share them, so a node reads the same either way.
_LABEL_NODE = "name"
_COMMIT_NODE = (
    "commit { oid authoredDate committedDate additions deletions "
    "author { user { login } } }"
)
_REVIEW_COMMENT_NODE = "id createdAt path body " + _AUTHOR
_REVIEW_NODE = (
    "id state submittedAt body "
    + _AUTHOR
    + " comments(first: 10) { totalCount "
    + _PAGE_INFO
    + " nodes { "
    + _REVIEW_COMMENT_NODE
    + " } }"
)
_ISSUE_COMMENT_NODE = "id createdAt body " + _AUTHOR
_FILE_NODE = "path additions deletions changeType"
_TIMELINE_ITEM_NODE = (
    "__typename "
    "... on ReadyForReviewEvent { createdAt actor { login } } "
    "... on ConvertToDraftEvent { createdAt actor { login } } "
    "... on ReviewRequestedEvent { createdAt actor { login } "
    + _REQUESTED_REVIEWER
    + " } "
    "... on ReviewRequestRemovedEvent { createdAt actor { login } "
    + _REQUESTED_REVIEWER
    + " } "
    "... on ClosedEvent { createdAt actor { login } } "
    "... on ReopenedEvent { createdAt actor { login } } "
    "... on MergedEvent { createdAt actor { login } } "
    "... on LabeledEvent { createdAt actor { login } label { name } } "
    "... on UnlabeledEvent { createdAt actor { login } label { name } } "
    "... on AutoMergeEnabledEvent { createdAt actor { login } } "
    "... on AutoMergeDisabledEvent { createdAt actor { login } }"
)


def _connection(field_with_args: str, node: str, *, total_count: bool = True) -> str:
    """Returns the selection for one connection field, for example
    "files(first: 50) { pageInfo { ... } nodes { path ... } }"."""
    return (
        field_with_args
        + " { "
        + ("totalCount " if total_count else "")
        + _PAGE_INFO
        + " nodes { "
        + node
        + " } }"
    )


PULL_REQUEST_FIELDS = (
    "fragment PullRequestFields on PullRequest { "
    "id number title state isDraft createdAt updatedAt closedAt mergedAt "
    "additions deletions changedFiles baseRefName headRefName isCrossRepository "
    + _AUTHOR
    + " mergedBy { login } mergeCommit { oid } "
    + _connection("labels(first: 20)", _LABEL_NODE)
    + " "
    + _connection("commits(first: 30)", _COMMIT_NODE)
    + " "
    + _connection("reviews(first: 10)", _REVIEW_NODE)
    + " "
    + _connection("comments(first: 20)", _ISSUE_COMMENT_NODE)
    + " "
    + _connection("files(first: 50)", _FILE_NODE, total_count=False)
    + " "
    + _connection(
        f"timelineItems(first: 30, itemTypes: [{_TIMELINE_ITEM_TYPES}])",
        _TIMELINE_ITEM_NODE,
    )
    + " }"
)

# Reads one repository's id, which fails or returns a null repository when the
# credential cannot see the repo. Search returns zero results instead of an error
# for such a repo, so the crawl runs this check first.
REPOSITORY_ACCESS_QUERY = (
    "query($owner: String!, $name: String!) { "
    + _RATE_LIMIT
    + " repository(owner: $owner, name: $name) { id } }"
)

PR_SEARCH_COUNT_QUERY = (
    "query($q: String!) { "
    + _RATE_LIMIT
    + " search(query: $q, type: ISSUE, first: 1) { issueCount } }"
)

PR_SEARCH_QUERY = (
    "query($q: String!, $after: String) { "
    + _RATE_LIMIT
    + " search(query: $q, type: ISSUE, first: 20, after: $after) { issueCount "
    + _PAGE_INFO
    + " nodes { ... on PullRequest { ...PullRequestFields } } } } "
    + PULL_REQUEST_FIELDS
)

SEARCH_CONNECTION_PATH: tuple[str, ...] = ("search",)


def _pull_request_overflow_query(
    field_with_args: str, node: str, *, total_count: bool = True
) -> str:
    return (
        "query($owner: String!, $name: String!, $number: Int!, $after: String) { "
        + _RATE_LIMIT
        + " repository(owner: $owner, name: $name) { pullRequest(number: $number) { "
        + _connection(field_with_args, node, total_count=total_count)
        + " } } }"
    )


@attr.define(frozen=True, kw_only=True)
class NestedConnection:
    """A connection that PULL_REQUEST_FIELDS reads the first page of, on the pull
    request or, for review comments, on each review."""

    name: str = attr.ib(validator=attr_validators.is_str)
    """The name pull_requests.truncated_connections records, for example
    issue_comments."""

    field: str = attr.ib(validator=attr_validators.is_str)
    """The GraphQL field that holds the connection, for example comments."""

    is_nested_in_reviews: bool = attr.ib(validator=attr_validators.is_bool)
    """Whether the connection is on each review rather than on the pull request."""

    overflow_query: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Reads the rest of the connection 100 nodes at a time, or None when the crawl
    records that the connection was cut off without reading the rest."""

    @property
    def connection_path(self) -> tuple[str, ...]:
        """Returns the path from overflow_query's data to the connection, as
        GitHubGraphQLClient.paginate takes it."""
        if self.is_nested_in_reviews:
            return ("node", self.field)
        return ("repository", "pullRequest", self.field)


# In the order pull_requests.truncated_connections lists them.
NESTED_CONNECTIONS: list[NestedConnection] = [
    NestedConnection(
        name="reviews",
        field="reviews",
        is_nested_in_reviews=False,
        overflow_query=_pull_request_overflow_query(
            "reviews(first: 100, after: $after)", _REVIEW_NODE
        ),
    ),
    NestedConnection(
        name="review_comments",
        field="comments",
        is_nested_in_reviews=True,
        overflow_query=(
            "query($reviewId: ID!, $after: String) { "
            + _RATE_LIMIT
            + " node(id: $reviewId) { ... on PullRequestReview { "
            + _connection("comments(first: 100, after: $after)", _REVIEW_COMMENT_NODE)
            + " } } }"
        ),
    ),
    NestedConnection(
        name="issue_comments",
        field="comments",
        is_nested_in_reviews=False,
        overflow_query=_pull_request_overflow_query(
            "comments(first: 100, after: $after)", _ISSUE_COMMENT_NODE
        ),
    ),
    NestedConnection(
        name="commits",
        field="commits",
        is_nested_in_reviews=False,
        overflow_query=_pull_request_overflow_query(
            "commits(first: 100, after: $after)", _COMMIT_NODE
        ),
    ),
    NestedConnection(
        name="files",
        field="files",
        is_nested_in_reviews=False,
        overflow_query=_pull_request_overflow_query(
            "files(first: 100, after: $after)", _FILE_NODE, total_count=False
        ),
    ),
    NestedConnection(
        name="timeline_items",
        field="timelineItems",
        is_nested_in_reviews=False,
        overflow_query=_pull_request_overflow_query(
            f"timelineItems(first: 100, after: $after, itemTypes: [{_TIMELINE_ITEM_TYPES}])",
            _TIMELINE_ITEM_NODE,
        ),
    ),
    # No pull request in these repos has more than the 20 labels the search query
    # reads; a longer list shows up in truncated_connections.
    NestedConnection(
        name="labels", field="labels", is_nested_in_reviews=False, overflow_query=None
    ),
]
