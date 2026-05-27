# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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

"""Helpers for talking to the Github API"""
import logging
import time

from github import Github
from github.Auth import Token
from more_itertools import one

from recidiviz.repo.issue_references import GithubIssue
from recidiviz.utils.github_pull_request import GithubPullRequest
from recidiviz.utils.secrets import get_secret
from recidiviz.utils.string_formatting import truncate_string_if_necessary

GITHUB_ISSUE_OR_COMMENT_BODY_MAX_LENGTH = 65536
GITHUB_HELPERBOT_TOKEN_SECRET_NAME = "github_deploy_script_pat"  # nosec

RECIDIVIZ_GITHUB_ORGANIZATION = "Recidiviz"
RECIDIVIZ_DATA_REPO = f"{RECIDIVIZ_GITHUB_ORGANIZATION}/pulse-data"
RECIDIVIZ_DASHBOARDS_REPO = f"{RECIDIVIZ_GITHUB_ORGANIZATION}/recidiviz-dashboards"
LOOKER_REPO_NAME = f"{RECIDIVIZ_GITHUB_ORGANIZATION}/looker"

HELPERBOT_USER_NAME = "helperbot-recidiviz"
__gh_helperbot_client = None


def github_helperbot_client() -> Github:
    global __gh_helperbot_client
    if not __gh_helperbot_client:
        access_token = get_secret(GITHUB_HELPERBOT_TOKEN_SECRET_NAME)  # nosec
        if access_token is None:
            raise KeyError("Couldn't locate helperbot access token")
        __gh_helperbot_client = Github(auth=Token(access_token))
    return __gh_helperbot_client


def get_pr_if_exists(
    github_client: Github,
    head_branch_name: str,
    base_branch_name: str,
    repo: str = RECIDIVIZ_DATA_REPO,
) -> str | None:
    """Returns the URL of the pull request if it exists, otherwise returns None."""
    repo_obj = github_client.get_repo(repo)
    existing_prs = repo_obj.get_pulls(
        state="open",
        head=f"{RECIDIVIZ_GITHUB_ORGANIZATION}:{head_branch_name}",
        base=base_branch_name,
    )
    if existing_prs and existing_prs.totalCount > 0:
        existing_pr = one(existing_prs)
        return existing_pr.html_url
    return None


def open_pr_if_not_exists(
    github_client: Github,
    title: str,
    body: str,
    head_branch_name: str,
    base_branch_name: str,
    repo: str = RECIDIVIZ_DATA_REPO,
) -> str:
    """Opens a pull request with the given title, body, head, and base branch
    if a pull request with the same head and base branch doesn't already exist.
    Returns the URL of the pull request."""
    logging.info(
        "Attempting to open PR with head_branch_name [%s] and base_branch_name [%s] against repo [%s]",
        head_branch_name,
        base_branch_name,
        repo,
    )
    repo_obj = github_client.get_repo(repo)

    existing_pr_url = get_pr_if_exists(
        github_client=github_client,
        head_branch_name=head_branch_name,
        base_branch_name=base_branch_name,
        repo=repo,
    )
    if existing_pr_url:
        logging.info("Pull request already exists at [%s] - returning", existing_pr_url)
        return existing_pr_url

    body_length_safe = truncate_string_if_necessary(
        body, max_length=GITHUB_ISSUE_OR_COMMENT_BODY_MAX_LENGTH
    )
    pr = repo_obj.create_pull(
        title=title,
        body=body_length_safe,
        head=head_branch_name,
        base=base_branch_name,
    )
    logging.info("Opened new PR [%s] with [%s] commits", pr.html_url, pr.commits)
    return pr.html_url


def upsert_issue_comment(
    github_client: Github,
    issue_number: int,
    body: str,
    prefix: str,
    repo: str = RECIDIVIZ_DATA_REPO,
) -> None:
    """Upsert a Helperbot comment on an issue or PR.

    PRs are issues in GitHub's data model: `repo.get_issue(num)` works for
    both, and the comments returned are the same set you'd see calling
    `pr.get_issue_comments()`.
    """
    issue = github_client.get_repo(repo).get_issue(issue_number)
    comments = [
        comment
        for comment in issue.get_comments()
        if comment.user.login == HELPERBOT_USER_NAME and comment.body.startswith(prefix)
    ]

    body_length_safe = truncate_string_if_necessary(
        body, max_length=GITHUB_ISSUE_OR_COMMENT_BODY_MAX_LENGTH
    )

    try:
        comment = one(comments)
        comment.edit(body=body_length_safe)
    except ValueError:
        issue.create_comment(body=body_length_safe)


def upsert_helperbot_comment(
    issue_number: int,
    body: str,
    prefix: str,
    repo: str = RECIDIVIZ_DATA_REPO,
) -> None:
    """Adds a comment with the given |body| to the specified issue (or PR), or overwrites an existing
    comment with the |body| if a Helperbot-issued comment starting with |prefix| is found. You must
    have google secrets access to use this function.
    """
    github_client = github_helperbot_client()
    upsert_issue_comment(
        github_client=github_client,
        repo=repo,
        issue_number=issue_number,
        body=body,
        prefix=prefix,
    )


def format_region_specific_ticket_title(
    *, region_code: str, environment: str, title: str
) -> str:
    """Formatting utility for a region-specific Github issue title. Please change this
    function with CAUTION as it will cause duplicate GitHub issues for airflow and
    validation failures.
    """
    return f"[{environment}][{region_code.upper()}] {title}"


def get_closing_github_issues(
    pr: GithubPullRequest, github_client: Github
) -> list[GithubIssue]:
    """Query the GitHub GraphQL API for Github issues that will be closed by a PR
    (via "Closes #XXXX" in the PR description).
    """
    query = """
    query issuesToClose($owner: String!, $repo: String!, $pr: Int!) {
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $pr) {
          closingIssuesReferences(first: 10) {
            nodes {
              repository { nameWithOwner }
              number
            }
          }
        }
      }
    }
    """
    _, data = github_client.requester.graphql_query(
        query,
        variables={"owner": pr.owner, "repo": pr.repo, "pr": pr.number},
    )

    nodes = data["data"]["repository"]["pullRequest"]["closingIssuesReferences"][
        "nodes"
    ]
    return [
        GithubIssue(repo=node["repository"]["nameWithOwner"], number=node["number"])
        for node in nodes
    ]


def get_pr_head_sha(pr: GithubPullRequest, github_client: Github) -> str:
    """Return the SHA of a pull request's head commit."""
    pull = github_client.get_repo(f"{pr.owner}/{pr.repo}").get_pull(pr.number)
    return pull.head.sha


def get_pr_body(pr: GithubPullRequest, github_client: Github) -> str:
    """Return the body (description) of a pull request."""
    pull = github_client.get_repo(f"{pr.owner}/{pr.repo}").get_pull(pr.number)
    return pull.body or ""


def get_short_commit_sha(commit_sha: str) -> str:
    """Get the short version of a commit SHA (first 7 characters)."""
    return commit_sha[:7]


def poll_for_pr_merge(
    github_client: Github,
    pr_branch: str,
    base_branch: str,
    repo: str,
    timeout_minutes: int,
    poll_interval_seconds: int = 30,
) -> None:
    """Poll for a PR to be merged, timing out after the specified minutes.

    Args:
        github_client: GitHub client to use for API calls
        pr_branch: The head branch name of the PR to poll for
        base_branch: The base branch name of the PR
        repo: The repository name (e.g., "Recidiviz/looker")
        timeout_minutes: Maximum time to wait for PR merge in minutes
        poll_interval_seconds: Time to wait between polls in seconds (default: 30)

    Raises:
        TimeoutError: If the PR is not merged within the timeout period
    """
    timeout_seconds = timeout_minutes * 60
    start_time = time.time()

    logging.info(
        "Polling for PR merge (repo: %s, branch: %s, base: %s, timeout: %d minutes)",
        repo,
        pr_branch,
        base_branch,
        timeout_minutes,
    )

    while True:
        elapsed = time.time() - start_time

        if elapsed > timeout_seconds:
            raise TimeoutError(
                f"Timed out after {timeout_minutes} minutes waiting for PR to be merged."
            )

        pr = get_pr_if_exists(
            github_client=github_client,
            head_branch_name=pr_branch,
            base_branch_name=base_branch,
            repo=repo,
        )

        if pr is None:
            logging.info(
                "PR has been merged or closed.",
            )
            return

        remaining = timeout_seconds - elapsed
        logging.info(
            "PR %s still open. Waiting %d seconds before next poll. "
            "Time remaining: %.1f minutes",
            pr,
            poll_interval_seconds,
            remaining / 60,
        )
        time.sleep(poll_interval_seconds)
