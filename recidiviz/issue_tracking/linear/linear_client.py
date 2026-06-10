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
"""Client for querying the Linear GraphQL API."""

import enum
import logging
from datetime import datetime
from typing import Any

import attr
import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue
from recidiviz.utils.types import assert_type

logger = logging.getLogger(__name__)

LINEAR_API_URL = "https://api.linear.app/graphql"

# Number of times to attempt a single Linear API request before giving up.
LINEAR_API_MAX_ATTEMPTS = 4


class LinearApiError(Exception):
    """Raised when the Linear API returns an error or is unreachable."""


class RetryableLinearApiError(LinearApiError):
    """A transient Linear API failure (a 5xx/429 response or a network error)
    worth retrying. Subclasses LinearApiError so callers that catch the latter
    still handle it once retries are exhausted."""


class LinkKind(enum.Enum):
    """The relationship between a PR attachment and a Linear issue.

    Linear stores this as a free-form string in attachment metadata (not a
    schema-enforced enum), but only two values are used by their GitHub
    integration: "closes" and "contributes".
    See https://linear.app/docs/github#link-issues-with-pull-requests
    """

    CLOSES = "closes"
    CONTRIBUTES = "contributes"


@attr.s(frozen=True, kw_only=True)
class LinearIssueInfo:
    """API-fetched details for a Linear issue. Wraps a LinearIssue identifier
    with additional fields (UUID, title) needed for mutations."""

    linear_issue: LinearIssue = attr.ib()
    uuid: str = attr.ib()
    title: str = attr.ib()

    @property
    def identifier(self) -> str:
        return self.linear_issue.issue_identifier

    @property
    def team_key(self) -> str:
        return self.linear_issue.team_prefix


@attr.s(frozen=True, kw_only=True)
class LinearAttachment:
    """A Linear attachment linking a PR to an issue."""

    id: str = attr.ib()
    issue_identifier: str = attr.ib()
    # None for attachments not created by Linear's GitHub integration, which
    # carry no linkKind in their metadata.
    link_kind: LinkKind | None = attr.ib()
    source: str | None = attr.ib()

    @property
    def linear_issue(self) -> LinearIssue:
        return LinearIssue.from_string(self.issue_identifier)

    @classmethod
    def from_response(cls, response: dict[str, Any]) -> "LinearAttachment":
        """Builds a LinearAttachment from a single attachmentsForURL response
        node. metadata is always present (Linear returns it as a non-null JSON
        object, deserialized into a dict), but its linkKind is absent for
        attachments not created by Linear's GitHub integration."""
        metadata = assert_type(response["metadata"], dict)
        raw_link_kind = metadata.get("linkKind")
        return cls(
            id=response["id"],
            issue_identifier=response["issue"]["identifier"],
            link_kind=LinkKind(raw_link_kind) if raw_link_kind is not None else None,
            source=metadata.get("source"),
        )


_triage_state_cache: dict[str, str] = {}


class LinearClient:
    """Client for querying the Linear GraphQL API."""

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    @retry(
        retry=retry_if_exception_type(RetryableLinearApiError),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(LINEAR_API_MAX_ATTEMPTS),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def query(self, query: str, variables: dict[str, Any]) -> dict:
        """Execute a GraphQL query against the Linear API.

        Transient failures (network errors and 5xx/429 responses, e.g. the
        Cloudflare 502s Linear occasionally returns) are retried with
        exponential backoff. Client errors (4xx) and GraphQL-level errors are
        not retried since they won't resolve on their own.
        """
        try:
            response = requests.post(
                LINEAR_API_URL,
                json={"query": query, "variables": variables},
                headers={
                    "Authorization": self.api_key,
                    "Content-Type": "application/json",
                },
                timeout=30,
            )
        except requests.RequestException as e:
            raise RetryableLinearApiError(f"Linear API request failed: {e}") from e

        if response.status_code == 429 or response.status_code >= 500:
            raise RetryableLinearApiError(
                f"Linear API returned status {response.status_code}: {response.text}"
            )
        if response.status_code != 200:
            raise LinearApiError(
                f"Linear API returned status {response.status_code}: {response.text}"
            )

        data = response.json()
        if "errors" in data:
            raise LinearApiError(f"Linear API returned errors: {data['errors']}")

        return data["data"]

    def _paginated_query(
        self,
        query: str,
        variables: dict[str, Any],
        connection_field: str,
    ) -> list[dict]:
        """Execute a paginated GraphQL query and return all nodes.

        Linear uses the Relay connection spec: every paginated field returns
        ``nodes``, ``pageInfo.hasNextPage``, and ``pageInfo.endCursor``. The
        query must accept an ``$after: String`` variable for the cursor.
        """
        all_nodes: list[dict] = []
        cursor: str | None = None
        page_num = 0
        while True:
            page_num += 1
            page_variables = {**variables}
            if cursor is not None:
                page_variables["after"] = cursor
            result = self.query(query, page_variables)

            connection = result[connection_field]
            all_nodes.extend(connection["nodes"])
            logger.info(
                "Fetched page %d of %r (%d total so far)",
                page_num,
                connection_field,
                len(all_nodes),
            )

            page_info = connection["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            cursor = page_info["endCursor"]

        return all_nodes

    def get_closing_issues(self, pr_url: str) -> list[LinearIssue]:
        """Query Linear for issues linked to a PR with linkKind == 'closes'.

        attachmentsForURL returns every attachment for the PR URL, including
        ones created outside Linear's GitHub integration (which carry no
        linkKind); those and "contributes" links are filtered out here.
        """
        return [
            attachment.linear_issue
            for attachment in self.get_all_pr_attachments(pr_url)
            if attachment.link_kind is LinkKind.CLOSES
        ]

    def resolve_linear_to_github(self, linear_issue: LinearIssue) -> GithubIssue | None:
        """Find the synced GitHub issue for a Linear issue."""
        query = """
        query($id: String!) {
            issue(id: $id) {
                attachments(filter: { sourceType: { eq: "github" } }) {
                    nodes { url }
                }
            }
        }
        """
        result = self.query(query, {"id": str(linear_issue)})

        nodes = result.get("issue", {}).get("attachments", {}).get("nodes", [])
        for node in nodes:
            try:
                return GithubIssue.from_url(node.get("url", ""))
            except ValueError:
                continue
        return None

    def resolve_github_to_linear(self, github_issue: GithubIssue) -> LinearIssue | None:
        """Find the synced Linear issue for a GitHub issue."""
        query = """
        query($url: String!) {
            attachmentsForURL(url: $url) {
                nodes {
                    issue { identifier }
                }
            }
        }
        """
        result = self.query(query, {"url": github_issue.url})

        nodes = result.get("attachmentsForURL", {}).get("nodes", [])
        if nodes:
            identifier = nodes[0]["issue"]["identifier"]
            return LinearIssue.from_string(identifier)
        return None

    def get_all_pr_attachments(self, pr_url: str) -> list[LinearAttachment]:
        """Fetch all Linear attachments for a PR URL."""
        query = """
        query($url: String!) {
            attachmentsForURL(url: $url) {
                nodes {
                    id
                    metadata
                    issue { identifier }
                }
            }
        }
        """
        result = self.query(query, {"url": pr_url})

        nodes = result.get("attachmentsForURL", {}).get("nodes", [])
        return [LinearAttachment.from_response(node) for node in nodes]

    def create_pr_attachment(
        self,
        issue_identifier: str,
        pr_url: str,
        pr_number: int,
        link_kind: LinkKind,
        source: str,
    ) -> str:
        """Create a Linear attachment linking a PR to an issue. Returns the
        attachment ID."""
        mutation = """
        mutation($input: AttachmentCreateInput!) {
            attachmentCreate(input: $input) {
                success
                attachment { id }
            }
        }
        """
        result = self.query(
            mutation,
            {
                "input": {
                    "issueId": issue_identifier,
                    "url": pr_url,
                    "title": f"PR #{pr_number}",
                    "metadata": {
                        "source": source,
                        "linkKind": link_kind.value,
                    },
                }
            },
        )
        return result["attachmentCreate"]["attachment"]["id"]

    def update_attachment(
        self, attachment_id: str, title: str, link_kind: LinkKind, source: str
    ) -> None:
        """Update an existing attachment."""
        mutation = """
        mutation($id: String!, $input: AttachmentUpdateInput!) {
            attachmentUpdate(id: $id, input: $input) {
                success
            }
        }
        """
        self.query(
            mutation,
            {
                "id": attachment_id,
                "input": {
                    "title": title,
                    "metadata": {
                        "source": source,
                        "linkKind": link_kind.value,
                    },
                },
            },
        )

    def delete_attachment(self, attachment_id: str) -> None:
        """Delete an attachment by ID."""
        mutation = """
        mutation($id: String!) {
            attachmentDelete(id: $id) {
                success
            }
        }
        """
        self.query(mutation, {"id": attachment_id})

    def get_recently_closed_issues(
        self,
        since: datetime,
        exclude_with_labels: list[str],
    ) -> list[LinearIssueInfo]:
        """Query Linear for issues completed or canceled since the given
        timestamp. Only returns issues still in a completed/canceled state
        (skips issues that have already been reopened).

        Linear tracks completion and cancellation with separate timestamps
        (a canceled issue has a null ``completedAt``), so we match either
        ``completedAt`` or ``canceledAt`` falling within the lookback window.

        Issues carrying any label in |exclude_with_labels| are excluded
        server-side via ``labels: { every: { name: { nin: ... } } }`` — i.e.
        every label on the issue must fall outside the excluded set. Issues
        with no labels match vacuously and are included.
        """
        if since.tzinfo is None or since.tzinfo.utcoffset(since) is None:
            raise ValueError(
                f"since must be timezone-aware, got naive datetime: {since}"
            )
        query = """
        query($since: DateTimeOrDuration!, $excludeLabels: [String!]!, $after: String) {
            issues(
                filter: {
                    state: { type: { in: ["completed", "canceled"] } }
                    labels: { every: { name: { nin: $excludeLabels } } }
                    or: [
                        { completedAt: { gte: $since } }
                        { canceledAt: { gte: $since } }
                    ]
                }
                first: 50
                after: $after
            ) {
                nodes {
                    id
                    identifier
                    title
                    team { key }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        """
        nodes = self._paginated_query(
            query,
            {"since": since.isoformat(), "excludeLabels": exclude_with_labels},
            "issues",
        )

        return [
            LinearIssueInfo(
                linear_issue=LinearIssue.from_string(node["identifier"]),
                uuid=node["id"],
                title=node["title"],
            )
            for node in nodes
        ]

    def _get_triage_state_id(self, team_key: str) -> str:
        """Return the ID of the triage workflow state for the given team.
        Results are cached per team key.

        Linear doesn't have a generic "reopen" action. Each team defines its
        own workflow states (each with a unique UUID), so we have to look up the
        right target state for the issue's team.
        """
        if team_key in _triage_state_cache:
            return _triage_state_cache[team_key]

        query = """
        query($teamKey: String!) {
            teams(filter: { key: { eq: $teamKey } }) {
                nodes {
                    states {
                        nodes {
                            id
                            type
                        }
                    }
                }
            }
        }
        """
        result = self.query(query, {"teamKey": team_key})

        teams = result["teams"]["nodes"]
        if not teams:
            raise LinearApiError(f"No team found with key {team_key!r}")

        for state in teams[0]["states"]["nodes"]:
            if state["type"] == "triage":
                _triage_state_cache[team_key] = state["id"]
                return state["id"]

        raise LinearApiError(f"No triage state found for team {team_key!r}")

    def reopen_issue(self, issue_info: LinearIssueInfo) -> None:
        """Reopen a completed/cancelled issue by moving it back to the team's
        triage state. Raises a LinearApiError if the team has no triage state
        (for now, we expect all teams we interact with to have one).

        Linear has no "reopen" API — issues live in typed workflow states
        (triage, backlog, started, completed, etc.) and each team defines its
        own states with unique IDs. Reopening means looking up the appropriate
        target state for the issue's team and transitioning to it via the
        ``issueUpdate`` mutation.
        """
        target_state_id = self._get_triage_state_id(issue_info.team_key)
        mutation = """
        mutation($id: String!, $input: IssueUpdateInput!) {
            issueUpdate(id: $id, input: $input) {
                success
            }
        }
        """
        self.query(
            mutation,
            {"id": issue_info.uuid, "input": {"stateId": target_state_id}},
        )

    def create_comment(self, issue_info: LinearIssueInfo, comment_body: str) -> None:
        """Post a comment on a Linear issue."""
        mutation = """
        mutation($input: CommentCreateInput!) {
            commentCreate(input: $input) {
                success
            }
        }
        """
        self.query(
            mutation,
            {"input": {"issueId": issue_info.uuid, "body": comment_body}},
        )
