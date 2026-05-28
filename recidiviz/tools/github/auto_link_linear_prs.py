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
"""Auto-links PRs referencing GitHub issue numbers (#XXXXX) to their synced
Linear issues.

Linear's native integration only links PRs that reference Linear IDs
(OBT-123456) with a magic word. This script fills the gap for #XXXXX
references by creating/updating/deleting Linear attachments as the PR
description changes.

Usage (called by .github/workflows/auto-link-linear-prs.yml):
  python -m recidiviz.tools.github.auto_link_linear_prs \
    --pr-url https://github.com/Recidiviz/pulse-data/pull/123 \
    --github-token $GITHUB_TOKEN \
    --linear-api-key $LINEAR_API_KEY

The --github-token and --linear-api-key flags are optional; if omitted, the
script falls back to the GITHUB_TOKEN and LINEAR_API_KEY environment variables.
"""

import argparse
import logging
import os
import re
import sys
from collections import defaultdict

from github import Github
from github.Auth import Token

from recidiviz.repo.issue import GithubIssue, LinearIssue
from recidiviz.repo.linear_client import LinearAttachment, LinearClient, LinkKind
from recidiviz.utils.github import get_pr_body
from recidiviz.utils.github_pull_request import GithubPullRequest

AUTO_LINK_SOURCE_MARKER = "auto-link-action"

# GitHub closing keywords.
# See https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/linking-a-pull-request-to-an-issue#linking-a-pull-request-to-an-issue-using-a-keyword
CLOSING_KEYWORDS = frozenset(
    {
        "close",
        "closes",
        "closed",
        "fix",
        "fixes",
        "fixed",
        "resolve",
        "resolves",
        "resolved",
    }
)

# Non-closing keywords recognized by Linear's GitHub integration. These create
# a "contributes" link but do not auto-close the issue on merge.
# See https://linear.app/docs/github#link-issues-with-pull-requests
NON_CLOSING_KEYWORDS = frozenset(
    {
        "part of",
        "related to",
        "contributes to",
        "addresses",
        "references",
        "for",
        "implements",
        "implementing",
        "completing",
        "closing",
    }
)

ALL_KEYWORDS = CLOSING_KEYWORDS | NON_CLOSING_KEYWORDS

# Sort longest-first so multi-word keywords like "contributes to" match before
# their prefixes (e.g. a hypothetical single-word "contributes").
_KEYWORD_ALTERNATION = "|".join(
    re.escape(kw) for kw in sorted(ALL_KEYWORDS, key=len, reverse=True)
)

# Matches: <keyword> [Owner/Repo]#<number>
# Examples: "Closes #123", "Part of Recidiviz/pulse-data#456"
_ISSUE_NUMBER_PATTERN = re.compile(
    rf"(?:^|(?<=\s))(?P<keyword>{_KEYWORD_ALTERNATION})"
    rf"\s+(?P<repo>\S+/\S+)?#(?P<issue_number>\d+)",
    re.IGNORECASE,
)

# Matches: <keyword> https://github.com/<owner>/<repo>/issues/<number>
# Examples: "Closes https://github.com/Recidiviz/pulse-data/issues/80250"
_ISSUE_URL_PATTERN = re.compile(
    rf"(?:^|(?<=\s))(?P<keyword>{_KEYWORD_ALTERNATION})"
    rf"\s+(?P<issue_url>https://github\.com/\S+/\S+/issues/\d+)",
    re.IGNORECASE,
)


def parse_issue_references(
    pr_body: str, pr: GithubPullRequest
) -> dict[LinkKind, list[GithubIssue]]:
    """Parse a PR body for issue references preceded by magic words.

    Detects both short references (#123, Owner/Repo#123) and full GitHub issue
    URLs. Returns a mapping from LinkKind to the list of referenced
    GithubIssues. If the same issue appears with both closing and non-closing
    keywords, closing wins.
    """
    issue_to_link_kind: dict[GithubIssue, LinkKind] = {}

    for match in _ISSUE_NUMBER_PATTERN.finditer(pr_body):
        keyword = match.group("keyword").lower()
        repo = match.group("repo") or f"{pr.owner}/{pr.repo}"
        issue = GithubIssue(repo=repo, number=int(match.group("issue_number")))
        link_kind = (
            LinkKind.CLOSES if keyword in CLOSING_KEYWORDS else LinkKind.CONTRIBUTES
        )
        if issue_to_link_kind.get(issue) != LinkKind.CLOSES:
            issue_to_link_kind[issue] = link_kind

    for match in _ISSUE_URL_PATTERN.finditer(pr_body):
        keyword = match.group("keyword").lower()
        try:
            issue = GithubIssue.from_url(match.group("issue_url"))
        except ValueError:
            continue
        link_kind = (
            LinkKind.CLOSES if keyword in CLOSING_KEYWORDS else LinkKind.CONTRIBUTES
        )
        if issue_to_link_kind.get(issue) != LinkKind.CLOSES:
            issue_to_link_kind[issue] = link_kind

    result: dict[LinkKind, list[GithubIssue]] = {}
    for issue, link_kind in issue_to_link_kind.items():
        result.setdefault(link_kind, []).append(issue)
    return result


def _reconcile_linear_attachments(
    *,
    pr: GithubPullRequest,
    all_attachments_for_pr: list[LinearAttachment],
    desired_linear_links: dict[LinkKind, list[LinearIssue]],
    linear_client: LinearClient,
) -> None:
    """Reconcile desired Linear issue attachments for the given PR with existing
    action-created Linear issue attachments.

    Creates new attachments for newly referenced issues, updates the linkKind
    on existing attachments when it changes (e.g. "contributes" -> "closes"),
    and deletes attachments whose references were removed from the PR body.
    Skips issues that already have a natively-created attachment.
    """
    action_created_attachments = [
        att for att in all_attachments_for_pr if att.source == AUTO_LINK_SOURCE_MARKER
    ]
    all_linked_identifiers = {att.issue_identifier for att in all_attachments_for_pr}

    existing_action_created_attachment_by_linear_issue: dict[
        LinearIssue, LinearAttachment
    ] = {}
    for att in action_created_attachments:
        linear_issue = att.linear_issue
        if linear_issue in existing_action_created_attachment_by_linear_issue:
            raise ValueError(
                f"Found PR {pr.url} attached multiple times to Linear issue "
                f"{linear_issue}"
            )
        existing_action_created_attachment_by_linear_issue[linear_issue] = att

    desired_attachment_kind_by_linear_issue: dict[LinearIssue, LinkKind] = {}
    for link_kind, issues in desired_linear_links.items():
        for issue in issues:
            desired_attachment_kind_by_linear_issue[issue] = link_kind

    for issue, att in existing_action_created_attachment_by_linear_issue.items():
        if issue not in desired_attachment_kind_by_linear_issue:
            logging.info(
                "Deleting attachment %s for %s (reference removed)",
                att.id,
                issue,
            )
            linear_client.delete_attachment(att.id)
        elif desired_attachment_kind_by_linear_issue[issue] != att.link_kind:
            logging.info(
                "Updating attachment for %s: %s -> %s",
                issue,
                att.link_kind,
                desired_attachment_kind_by_linear_issue[issue],
            )
            linear_client.update_attachment(
                att.id,
                f"PR #{pr.number}",
                desired_attachment_kind_by_linear_issue[issue],
                AUTO_LINK_SOURCE_MARKER,
            )

    for issue, link_kind in desired_attachment_kind_by_linear_issue.items():
        if issue in existing_action_created_attachment_by_linear_issue:
            continue
        if issue.issue_identifier in all_linked_identifiers:
            logging.info("Skipping %s — already linked by another source", issue)
            continue
        logging.info("Creating attachment for %s with linkKind=%s", issue, link_kind)
        linear_client.create_pr_attachment(
            issue.issue_identifier,
            pr.url,
            pr.number,
            link_kind,
            AUTO_LINK_SOURCE_MARKER,
        )


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Auto-link PRs with #XXXXX references to Linear issues."
    )
    parser.add_argument("--pr-url", required=True, help="URL of the pull request.")
    parser.add_argument(
        "--github-token",
        default=None,
        help="GitHub token for fetching the PR body. "
        "Falls back to the GITHUB_TOKEN environment variable.",
    )
    parser.add_argument(
        "--linear-api-key",
        default=None,
        help="API key for querying Linear. "
        "Falls back to the LINEAR_API_KEY environment variable.",
    )
    return parser


def main(*, pr_url: str, github_token: str, linear_api_key: str) -> int:
    """Auto-link PRs with #XXXXX references to Linear issues. Returns a non-zero
    exit code on failure."""
    pr = GithubPullRequest.from_url(pr_url)
    linear_client = LinearClient(linear_api_key)
    github_client = Github(auth=Token(github_token))
    pr_body = get_pr_body(pr, github_client)

    issue_refs = parse_issue_references(pr_body, pr)
    if not issue_refs:
        logging.info("No keyword + #XXXXX references found in PR body")

    desired_linear_links: dict[LinkKind, list[LinearIssue]] = defaultdict(list)
    for link_kind, github_issues in issue_refs.items():
        for github_issue in github_issues:
            linear_issue = linear_client.resolve_github_to_linear(github_issue)
            if linear_issue is None:
                logging.info("No synced Linear issue for %s — skipping", github_issue)
                continue
            desired_linear_links[link_kind].append(linear_issue)

    all_attachments = linear_client.get_all_pr_attachments(pr_url)

    _reconcile_linear_attachments(
        desired_linear_links=desired_linear_links,
        all_attachments_for_pr=all_attachments,
        pr=pr,
        linear_client=linear_client,
    )
    return 0


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
