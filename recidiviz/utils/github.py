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

from github import Github
from more_itertools import one

from recidiviz.utils.secrets import get_secret
from recidiviz.utils.string_formatting import truncate_string_if_necessary

GITHUB_ISSUE_OR_COMMENT_BODY_MAX_LENGTH = 65536
GITHUB_HELPERBOT_TOKEN_SECRET_NAME = "github_deploy_script_pat"  # nosec

RECIDIVIZ_GITHUB_ORGANIZATION = "Recidiviz"
RECIDIVIZ_DATA_REPO = f"{RECIDIVIZ_GITHUB_ORGANIZATION}/pulse-data"
LOOKER_REPO_NAME = f"{RECIDIVIZ_GITHUB_ORGANIZATION}/looker"

HELPERBOT_USER_NAME = "helperbot-recidiviz"
__gh_helperbot_client = None


def github_helperbot_client() -> Github:
    global __gh_helperbot_client
    if not __gh_helperbot_client:
        access_token = get_secret(GITHUB_HELPERBOT_TOKEN_SECRET_NAME)  # nosec
        if access_token is None:
            raise KeyError("Couldn't locate helperbot access token")
        __gh_helperbot_client = Github(access_token)
    return __gh_helperbot_client


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
    repo_obj = github_client.get_repo(repo)
    existing_prs = repo_obj.get_pulls(
        state="open",
        head=f"{RECIDIVIZ_GITHUB_ORGANIZATION}:{head_branch_name}",
        base=base_branch_name,
    )
    if existing_prs:
        return one(existing_prs).html_url

    body_length_safe = truncate_string_if_necessary(
        body, max_length=GITHUB_ISSUE_OR_COMMENT_BODY_MAX_LENGTH
    )
    pr = repo_obj.create_pull(
        title=title,
        body=body_length_safe,
        head=head_branch_name,
        base=base_branch_name,
    )
    return pr.html_url


def upsert_pr_comment(
    github_client: Github,
    pull_request_number: int,
    body: str,
    prefix: str,
) -> None:
    pull_request = github_client.get_repo(RECIDIVIZ_DATA_REPO).get_pull(
        pull_request_number
    )
    comments = [
        comment
        for comment in pull_request.get_issue_comments()
        if comment.user.login == HELPERBOT_USER_NAME and comment.body.startswith(prefix)
    ]

    body_length_safe = truncate_string_if_necessary(
        body, max_length=GITHUB_ISSUE_OR_COMMENT_BODY_MAX_LENGTH
    )

    try:
        comment = one(comments)
        comment.edit(body=body_length_safe)
    except ValueError:
        pull_request.create_issue_comment(body=body_length_safe)


def upsert_helperbot_comment(pull_request_number: int, body: str, prefix: str) -> None:
    """Adds a comment with the given |body| to the specified pull request, or overwrites an existing comment
    with the |body| if a Helperbot-issued comment starting with |existing_comment_prefix| is found. You must
    have google secrets access to use this function.
    """
    github_client = github_helperbot_client()
    upsert_pr_comment(
        github_client=github_client,
        pull_request_number=pull_request_number,
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
