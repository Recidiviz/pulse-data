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
"""Given a PR, finds codebase TODOs that reference issues closed by that PR.

Collects closing issues from two sources:
  - GitHub "Closes #XXXX" references (queried via GitHub GraphQL API)
  - Linear attachments with linkKind "closes" (queried via Linear GraphQL API)

Cross-references between GitHub and Linear issues are resolved so that a TODO
referencing either side of a linked pair is caught.

Exits with code 1 if any matching TODOs are found, or if the Linear API is
unreachable (fail-closed).

Usage (called by .github/workflows/find-linked-todos.yml):
  python -m recidiviz.tools.github.find_closing_issue_todos \
    --pr-url https://github.com/Recidiviz/pulse-data/pull/123 \
    --github-token $GITHUB_TOKEN \
    --linear-api-key $LINEAR_API_KEY

The --github-token and --linear-api-key flags are optional; if omitted, the
script falls back to the GITHUB_TOKEN and LINEAR_API_KEY environment variables.
"""

import argparse
import logging
import os
import sys

from github import Github
from github.Auth import Token

from recidiviz.github.github_client import get_closing_github_issues, get_pr_head_sha
from recidiviz.github.github_issue import GithubIssue
from recidiviz.github.github_pull_request import GithubPullRequest
from recidiviz.issue_tracking.codebase_todos import (
    CodeReference,
    get_entire_codebase_issue_references,
    to_markdown,
)
from recidiviz.issue_tracking.issue import Issue
from recidiviz.issue_tracking.linear.linear_client import LinearClient
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(
        description="Finds codebase TODOs referencing issues closed by a PR."
    )
    parser.add_argument(
        "--pr-url",
        required=True,
        help="URL of the pull request.",
    )
    parser.add_argument(
        "--github-token",
        default=None,
        help="GitHub token for querying the GitHub GraphQL API. "
        "Falls back to the GITHUB_TOKEN environment variable.",
    )
    parser.add_argument(
        "--linear-api-key",
        default=None,
        help="API key for querying Linear. "
        "Falls back to the LINEAR_API_KEY environment variable.",
    )
    return parser


def _get_closing_issues(
    pr: GithubPullRequest,
    github_client: Github,
    linear_client: LinearClient,
) -> set[GithubIssue | LinearIssue]:
    """Build the unified set of closing issue identifiers from both GitHub and
    Linear sources, resolving cross-references between the two systems. If a Github
    issue is synced to Linear, the returned list will contain both a GithubIssue and
    LinearIssue entry for that issue.
    """
    directly_closing_issues: set[GithubIssue | LinearIssue] = set()
    directly_closing_issues.update(get_closing_github_issues(pr, github_client))
    directly_closing_issues.update(linear_client.get_closing_issues(pr.url))

    all_closing_issues: set[GithubIssue | LinearIssue] = set(directly_closing_issues)
    for issue in directly_closing_issues:
        linked_issue: GithubIssue | LinearIssue | None
        if isinstance(issue, GithubIssue):
            linked_issue = linear_client.resolve_github_to_linear(issue)
        elif isinstance(issue, LinearIssue):
            linked_issue = linear_client.resolve_linear_to_github(issue)
        else:
            raise ValueError(f"Unexpected issue type {issue}")
        if linked_issue:
            all_closing_issues.add(linked_issue)
    return all_closing_issues


def main(
    *,
    pr_url: str,
    github_token: str,
    linear_api_key: str,
) -> int:
    """Finds codebase TODOs referencing issues closed by the given PR. Returns a
    non-zero exit code if any are found."""
    pr = GithubPullRequest.from_url(pr_url)
    github_client = Github(auth=Token(github_token))
    pr_head_sha = get_pr_head_sha(pr, github_client)
    codebase_issue_references = get_entire_codebase_issue_references(
        commit_ref=pr_head_sha
    )
    linear_client = LinearClient(linear_api_key)

    closing_issues = _get_closing_issues(pr, github_client, linear_client)

    matching_references: dict[Issue, list[CodeReference]] = {
        issue: codebase_issue_references[issue]
        for issue in closing_issues
        if issue in codebase_issue_references
    }

    if not matching_references:
        print("No matching references found")
        return 0

    print("Found the following matching issue references:")
    print()
    print(to_markdown(matching_references))
    return 1


def _resolve_token(arg_value: str | None, arg_name: str, env_var: str) -> str:
    """Returns the CLI arg if provided, otherwise falls back to the environment
    variable. Raises ValueError if neither is set."""
    value = arg_value or os.environ.get(env_var)
    if not value:
        raise ValueError(
            f"Must provide --{arg_name} or set the {env_var} environment variable."
        )
    return value


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = create_parser().parse_args()
    sys.exit(
        main(
            pr_url=args.pr_url,
            github_token=_resolve_token(
                args.github_token, "github-token", "GITHUB_TOKEN"
            ),
            linear_api_key=_resolve_token(
                args.linear_api_key, "linear-api-key", "LINEAR_API_KEY"
            ),
        )
    )
