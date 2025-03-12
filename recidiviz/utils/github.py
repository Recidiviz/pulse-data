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

GITHUB_HELPERBOT_TOKEN_SECRET_NAME = "github_deploy_script_pat"  # nosec
RECIDIVIZ_DATA_REPO = "Recidiviz/pulse-data"
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


def upsert_helperbot_comment(pull_request_number: int, body: str, prefix: str) -> None:
    """Adds a comment with the given |body| to the specified pull request, or overwrites an existing comment
    with the |body| if a Helperbot-issued comment starting with |existing_comment_prefix| is found.
    """
    github_client = github_helperbot_client()
    pull_request = github_client.get_repo(RECIDIVIZ_DATA_REPO).get_pull(
        pull_request_number
    )
    comments = [
        comment
        for comment in pull_request.get_issue_comments()
        if comment.user.login == HELPERBOT_USER_NAME and comment.body.startswith(prefix)
    ]

    try:
        comment = one(comments)
        comment.edit(body=body)
    except ValueError:
        pull_request.create_issue_comment(body=body)


def format_region_specific_ticket_title(
    *, region_code: str, environment: str, title: str
) -> str:
    """Fomatting utility for a region-specifc Github issue title."""
    return f"[{environment}][{region_code.upper()}] {title}"
