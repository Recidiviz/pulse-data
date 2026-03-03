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
"""Mixin for GitHub-related functionality."""
import datetime

import attr
from airflow.providers.github.hooks.github import GithubHook
from github import GithubObject
from github.Issue import Issue
from github.IssueComment import IssueComment
from github.Repository import Repository

from recidiviz.utils.github import HELPERBOT_USER_NAME, RECIDIVIZ_DATA_REPO


@attr.define
class RecidivizGithubMixin:
    """Mixin for GitHub-related functionality."""

    _hook: GithubHook = attr.ib(init=False)
    _repo: Repository = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self._hook = GithubHook()
        self._repo = self._hook.get_conn().get_repo(RECIDIVIZ_DATA_REPO)

    def create_new_issue(self, title: str, body: str, labels: list[str]) -> Issue:
        """Creates a new GitHub issue with the given title, body, and labels."""
        issue = self._repo.create_issue(
            title=title,
            body=body,
            labels=labels,
        )
        return issue

    @staticmethod
    def close_issue(issue: Issue, comment_body: str) -> None:
        """Closes a GitHub issue with a comment."""
        if issue.state == "open":
            issue.create_comment(comment_body)
            issue.edit(state="closed", state_reason="completed")

    @staticmethod
    def get_helperbot_comments(
        issue: Issue, comment_prefix: str | None = None
    ) -> list[IssueComment]:
        """Returns a list of comments on |issue| created by HELPERBOT_USER_NAME,
        optionally filtered by a comment prefix.

        Args:
            issue: The GitHub issue to retrieve comments from.
            comment_prefix: If provided, only comments starting with this prefix are returned.
        """
        return [
            comment
            for comment in issue.get_comments()
            if comment.user.login == HELPERBOT_USER_NAME
            and (comment_prefix is None or comment.body.startswith(comment_prefix))
        ]

    def _get_helperbot_issues(
        self,
        labels: list[str],
        state: str = "all",
        since: datetime.datetime | None = None,
    ) -> list[Issue]:
        return list(
            self._repo.get_issues(
                sort="created",
                direction="desc",
                labels=labels,
                # TODO(PyGithub/PyGithub#3084): remove mypy exemption once issue is fixed
                creator=HELPERBOT_USER_NAME,  # type: ignore
                state=state,
                since=since if since is not None else GithubObject.NotSet,
            )
        )

    def get_helperbot_issues_for_title(
        self,
        title: str,
        labels: list[str],
        state: str = "all",
        since: datetime.datetime | None = None,
    ) -> list[Issue]:
        """Returns a list of issues matching the given filters created by HELPERBOT_USER_NAME.

        Args:
            title: The exact title of the issue(s) to return.
            labels: The labels that the issue(s) must have.
            state: The state of the issue(s) to return (open, closed, or all).
            since: If provided, only issues updated at or after this time are returned.
        """
        matching_issues = self._get_helperbot_issues(
            labels=labels, state=state, since=since
        )
        return [issue for issue in matching_issues if issue.title == title]

    def get_helperbot_issues_for_title_prefix(
        self, title_prefix: str, labels: list[str], state: str = "all"
    ) -> list[Issue]:
        """Returns a list of issues matching the given filters created by HELPERBOT_USER_NAME.

        Args:
            title_prefix: The prefix of the title of the issue(s) to return.
            labels: The labels that the issue(s) must have.
            state: The state of the issue(s) to return (open, closed, or all).
        """
        matching_issues = self._get_helperbot_issues(labels=labels, state=state)
        return [
            issue for issue in matching_issues if issue.title.startswith(title_prefix)
        ]
