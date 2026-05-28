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
import json
from typing import Any

import attr
import requests

from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue

LINEAR_API_URL = "https://api.linear.app/graphql"


class LinearApiError(Exception):
    """Raised when the Linear API returns an error or is unreachable."""


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
class LinearAttachment:
    """A Linear attachment linking a PR to an issue."""

    id: str = attr.ib()
    issue_identifier: str = attr.ib()
    link_kind: LinkKind = attr.ib()
    source: str | None = attr.ib()

    @property
    def linear_issue(self) -> LinearIssue:
        return LinearIssue.from_string(self.issue_identifier)


class LinearClient:
    """Client for querying the Linear GraphQL API."""

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def _query(self, query: str, variables: dict[str, Any]) -> dict:
        """Execute a GraphQL query against the Linear API."""
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
            raise LinearApiError(f"Linear API request failed: {e}") from e

        if response.status_code != 200:
            raise LinearApiError(
                f"Linear API returned status {response.status_code}: {response.text}"
            )

        data = response.json()
        if "errors" in data:
            raise LinearApiError(f"Linear API returned errors: {data['errors']}")

        return data["data"]

    @staticmethod
    def _parse_metadata(node: dict) -> dict:
        """Parse the metadata field from a Linear API response node."""
        metadata = node.get("metadata", {})
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        return metadata

    def get_closing_issues(self, pr_url: str) -> list[LinearIssue]:
        """Query Linear for issues linked to a PR with linkKind == 'closes'."""
        query = """
        query($url: String!) {
            attachmentsForURL(url: $url) {
                nodes {
                    metadata
                    issue { identifier }
                }
            }
        }
        """
        result = self._query(query, {"url": pr_url})

        closing_issues: list[LinearIssue] = []
        nodes = result.get("attachmentsForURL", {}).get("nodes", [])
        for node in nodes:
            metadata = self._parse_metadata(node)
            if LinkKind(metadata.get("linkKind")) == LinkKind.CLOSES:
                identifier = node["issue"]["identifier"]
                closing_issues.append(LinearIssue.from_string(identifier))

        return closing_issues

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
        result = self._query(query, {"id": str(linear_issue)})

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
        result = self._query(query, {"url": github_issue.url})

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
        result = self._query(query, {"url": pr_url})

        attachments: list[LinearAttachment] = []
        nodes = result.get("attachmentsForURL", {}).get("nodes", [])
        for node in nodes:
            metadata = self._parse_metadata(node)
            attachments.append(
                LinearAttachment(
                    id=node["id"],
                    issue_identifier=node["issue"]["identifier"],
                    link_kind=LinkKind(metadata["linkKind"]),
                    source=metadata.get("source"),
                )
            )
        return attachments

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
        result = self._query(
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
        self._query(
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
        self._query(mutation, {"id": attachment_id})
