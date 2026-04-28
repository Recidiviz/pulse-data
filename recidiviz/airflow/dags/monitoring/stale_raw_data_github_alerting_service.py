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
"""GitHub alerting service specifically for stale raw data file monitoring."""
import datetime
import logging

import attr
from github.Issue import Issue
from more_itertools import one

from recidiviz.airflow.dags.monitoring.recidiviz_alerting_service import (
    RecidivizAlertingService,
)
from recidiviz.airflow.dags.monitoring.recidiviz_github_mixin import (
    RecidivizGithubMixin,
)
from recidiviz.airflow.dags.monitoring.stale_raw_data_alerting_incident import (
    StaleRawDataAlertingIncident,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import get_environment_for_project
from recidiviz.utils.string import StrictStringFormatter

STALE_RAW_DATA_LABELS: list[str] = ["Stale Raw Data", "Team: State Pod"]
DEFAULT_ISSUE_SEARCH_LOOKBACK_WINDOW_DAYS = 45


@attr.define
class StaleRawDataGitHubService(
    RecidivizAlertingService[StaleRawDataAlertingIncident], RecidivizGithubMixin
):
    """GitHub alerting service specifically for stale raw data file monitoring."""

    name: str
    state_code: StateCode
    project_id: str
    issue_labels: list[str]
    issue_search_lookback_window_days: int

    _cached_issues: list[Issue] | None = attr.ib(init=False, default=None)

    @classmethod
    def get_stale_raw_data_service_for_state_code(
        cls, *, project_id: str, state_code: StateCode
    ) -> "StaleRawDataGitHubService":
        label_for_environment = get_environment_for_project(project_id).value.title()
        label_for_state_code = f"Region: {state_code.value}"
        return StaleRawDataGitHubService(
            name=f"Stale Raw Data {state_code.value} Github Service",
            state_code=state_code,
            project_id=project_id,
            issue_labels=[
                *STALE_RAW_DATA_LABELS,
                label_for_state_code,
                label_for_environment,
            ],
            issue_search_lookback_window_days=DEFAULT_ISSUE_SEARCH_LOOKBACK_WINDOW_DAYS,
        )

    @property
    def cached_gh_issues(self) -> list[Issue]:
        """Returns all helperbot issues for this service's labels, fetching from
        GitHub only on the first call and caching for subsequent calls."""
        if self._cached_issues is None:
            since = datetime.datetime.now(
                tz=datetime.timezone.utc
            ) - datetime.timedelta(days=self.issue_search_lookback_window_days)
            self._cached_issues = self.get_helperbot_issues(
                labels=self.issue_labels, state="all", since=since
            )
        return self._cached_issues

    def _get_issue_title_prefix(
        self,
    ) -> str:
        environment = get_environment_for_project(self.project_id).value.title()
        return f"[{self.state_code.value}] [{environment}]"

    def _get_issue_title(self, incident: StaleRawDataAlertingIncident) -> str:
        return f"{self._get_issue_title_prefix()} {incident.unique_incident_id}"

    def _search_for_existing_incident_issues(
        self, incident: StaleRawDataAlertingIncident
    ) -> list[Issue]:
        """Searches for existing issues for the incident."""
        title = self._get_issue_title(incident)
        return [i for i in self.cached_gh_issues if i.title == title]

    def _search_for_open_issues_for_prefix(
        self, file_tag_incident_prefix: str
    ) -> list[Issue]:
        """Searches for any open issues for the file tag/environment/file_tag_incident_prefix combination."""
        title_prefix = f"{self._get_issue_title_prefix()} {file_tag_incident_prefix}"
        return [
            i
            for i in self.cached_gh_issues
            if i.state == "open" and i.title.startswith(title_prefix)
        ]

    def _handle_resolved_incident(self, incident: StaleRawDataAlertingIncident) -> None:
        """If a file is fresh, we want to close any open issues for that file/environment combination.
        We search by file tag prefix rather than unique incident id since the incident id includes the most recent import date,
        and the most recent import date will have changed by the time the incident is resolved.
        """
        matching_issues = self._search_for_open_issues_for_prefix(
            incident.file_tag_incident_prefix
        )

        if not matching_issues:
            logging.info(
                "File [%s][%s] is current and no open issues exist - no action needed",
                incident.state_code,
                incident.file_tag,
            )
            return

        comment = StrictStringFormatter().format(
            "File is now current. Last import: `{most_recent_import_date}`. Closing issue.",
            most_recent_import_date=incident.most_recent_import_date,
        )

        for issue in matching_issues:
            self.close_issue(issue, comment)
            logging.info(
                "Closed issue [#%s] for [%s][%s] - file is current",
                issue.number,
                incident.state_code,
                incident.file_tag,
            )

    def _create_new_stale_data_issue(
        self, incident: StaleRawDataAlertingIncident
    ) -> None:
        """Creates a new GitHub issue for a stale data incident."""
        body = StrictStringFormatter().format(
            "Raw data file `{file_tag}` for `{state_code}` is **{hours_stale:.1f} hours stale**.\n\n"
            "**Last successful import:** `{most_recent_import_date}`",
            file_tag=incident.file_tag,
            state_code=incident.state_code,
            hours_stale=incident.hours_stale,
            most_recent_import_date=incident.most_recent_import_date,
        )
        title = self._get_issue_title(incident)
        issue = self.create_new_issue(title, body, self.issue_labels)
        logging.info(
            "Created new issue [#%s] for stale file [%s][%s]",
            issue.number,
            incident.state_code,
            incident.file_tag,
        )

    def _update_existing_stale_data_issues(
        self, existing_issues: list[Issue], incident: StaleRawDataAlertingIncident
    ) -> None:
        """Updates an existing GitHub issue for an ongoing stale data incident. Closes duplicate issues if multiple exist.

        Edits the most recent helperbot comment if one exists, otherwise creates a new comment.
        This prevents comment spam since the monitoring runs hourly.
        """
        # The GitHub API is flaky and sometimes doesn't return an issue
        # when queried, causing us to accidentally open duplicates.
        # Keep the oldest and ensure any others are closed.
        existing_issues.sort(key=lambda i: i.created_at)
        issue = existing_issues[0]
        for duplicate in existing_issues[1:]:
            self.close_issue(
                duplicate,
                f"Closing duplicate issue in favor of #{issue.number}.",
            )
            logging.info(
                "Closed duplicate issue [#%s] for [%s][%s]",
                duplicate.number,
                incident.state_code,
                incident.file_tag,
            )

        if issue.state == "closed":
            issue.edit(state="open")
            logging.info(
                "Re-opened issue [#%s] for recurring staleness of [%s][%s]",
                issue.number,
                incident.state_code,
                incident.file_tag,
            )

        comment_header = "File is still stale:"
        full_comment = StrictStringFormatter().format(
            "{comment_header} **{hours_stale:.1f} hours** over threshold.\n\n"
            "**Last successful import:** `{most_recent_import_date}`",
            comment_header=comment_header,
            hours_stale=incident.hours_stale,
            most_recent_import_date=incident.most_recent_import_date,
        )

        helperbot_comments = self.get_helperbot_comments(
            issue, comment_prefix=comment_header
        )

        if helperbot_comments:
            helperbot_comment = one(helperbot_comments)
            helperbot_comment.edit(full_comment)
            logging.info(
                "Updated existing comment on issue [#%s] for stale file [%s][%s]",
                issue.number,
                incident.state_code,
                incident.file_tag,
            )
        else:
            issue.create_comment(full_comment)
            logging.info(
                "Created new comment on issue [#%s] for stale file [%s][%s]",
                issue.number,
                incident.state_code,
                incident.file_tag,
            )

    def handle_incident(self, incident: StaleRawDataAlertingIncident) -> None:
        """Handles a stale raw data incident by creating, updating, or closing GitHub issues.

        For ongoing incidents:
        - Creates a new issue if one doesn't exist
        - Updates and reopens an existing issue if the file remains stale

        For resolved incidents:
        - Closes all open issues for the file tag/environment combination
        """
        if incident.is_resolved:
            self._handle_resolved_incident(incident)
        else:
            existing_issues = self._search_for_existing_incident_issues(incident)
            if not existing_issues:
                self._create_new_stale_data_issue(incident)
            else:
                self._update_existing_stale_data_issues(existing_issues, incident)
