# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Information about GitHub Alerting service."""
import datetime
import logging

import attr
from airflow.providers.github.hooks.github import GithubHook
from github.Issue import Issue

from recidiviz.airflow.dags.monitoring.airflow_alerting_incident import (
    AirflowAlertingIncident,
)
from recidiviz.airflow.dags.monitoring.recidiviz_alerting_service import (
    RecidivzAlertingService,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.types import assert_type

RECIDIVIZ_DATA_REPO = "Recidiviz/pulse-data"
HELPERBOT_USER_NAME = "helperbot-recidiviz"

RAW_DATA_DEFAULT_LABELS = ["Raw Data Import Failure", "Team: State Pod"]


@attr.define
class RecidivizGitHubService(RecidivzAlertingService):
    """An alerting service that creates and updates issues in GitHub."""

    issue_labels: list[str]

    def __attrs_post_init__(self) -> None:
        self._hook = GithubHook()
        self._repo = self._hook.get_conn().get_repo(RECIDIVIZ_DATA_REPO)

    @classmethod
    def raw_data_service_for_state_code(
        cls, *, project_id: str, state_code: StateCode
    ) -> "RecidivizGitHubService":
        label_for_project = (
            "Prod" if project_id == GCP_PROJECT_PRODUCTION else "Staging"
        )
        label_for_state_code = f"Region: {state_code.value}"
        return RecidivizGitHubService(
            issue_labels=[
                *RAW_DATA_DEFAULT_LABELS,
                label_for_state_code,
                label_for_project,
            ]
        )

    def _search_past_issues_for_incident(
        self, incident: AirflowAlertingIncident
    ) -> Issue | None:
        matching_issues = self._repo.get_issues(
            sort="created",
            direction="desc",
            labels=self.issue_labels,
            # TODO(PyGithub/PyGithub#3084): remove mypy exemption once issue is fixed
            creator=HELPERBOT_USER_NAME,  # type: ignore
            state="all",
            # if an incident has been open for longer than our lookback period, we will
            # think it's a "new" incident since we use incident_start_date in the
            # incident id, so we should be safe to filter down issues to when this
            # incident started.
            since=incident.incident_start_date,
        )

        for issue in matching_issues:
            # match by unique_incident_id
            if issue.title == incident.unique_incident_id:
                return issue

        return None

    def _ensure_issue_closed_for_resolved_incident(
        self, issue: Issue | None, incident: AirflowAlertingIncident
    ) -> None:
        if issue and issue.state == "open":
            issue.create_comment(
                f"Successful job completion found on [`{assert_type(incident.next_success_date, datetime.datetime).isoformat()}`]; closing issue."
            )
            issue.edit(state="closed", state_reason="completed")
            logging.info(
                "Closed issue [#%s] for incident [%s] as a job run completed successfully on [%s]",
                issue.number,
                incident.unique_incident_id,
                assert_type(incident.next_success_date, datetime.datetime).isoformat(),
            )
        else:
            logging.info("Issue doesn't exist or is already closed")

    @staticmethod
    def _format_incident_issue_description_header(
        incident: AirflowAlertingIncident,
    ) -> str:
        date_strs = ", ".join(
            [
                f"`{failure_date.isoformat()}`"
                for failure_date in incident.failed_execution_dates
            ]
        )
        return f"Failed run of [`{incident.job_id}`] on the following dates: [ {date_strs} ]."

    @classmethod
    def _format_incident_issue_description(
        cls, incident: AirflowAlertingIncident
    ) -> str:
        error_message = (
            f" Most recent error message:\n```{incident.error_message}```"
            if incident.error_message
            else ""
        )
        return (
            f"{cls._format_incident_issue_description_header(incident)}{error_message}"
        )

    def _open_new_issue(self, incident: AirflowAlertingIncident) -> None:
        issue = self._repo.create_issue(
            title=incident.unique_incident_id,
            body=self._format_incident_issue_description(incident),
            labels=self.issue_labels,
        )
        logging.info(
            "Created new issue [#%s] for [%s]",
            issue.number,
            incident.unique_incident_id,
        )

    @staticmethod
    def _format_incident_issue_comment_header(incident: AirflowAlertingIncident) -> str:
        return f"Failure for [`{incident.job_id}`] on [ `{incident.most_recent_failure.isoformat()}` ]"

    @classmethod
    def _format_incident_issue_comment(cls, incident: AirflowAlertingIncident) -> str:
        error_message = (
            f" Most recent error message:\n```{incident.error_message}```"
            if incident.error_message
            else ""
        )
        return f"{cls._format_incident_issue_comment_header(incident)}{error_message}"

    def _update_existing_issue_for_ongoing_incident(
        self, issue: Issue, incident: AirflowAlertingIncident
    ) -> None:
        """Determines if an update to |issue| is necessary by reconciling comments on
        |issue| with failure dates on |incident|.
        """
        description_header = self._format_incident_issue_description_header(incident)

        incident_not_updated_since_creation = issue.body.startswith(description_header)

        most_recent_comment_header = self._format_incident_issue_comment_header(
            incident
        )

        comments = [
            comment
            for comment in issue.get_comments()
            if comment.user.login == HELPERBOT_USER_NAME
            and comment.body.startswith(most_recent_comment_header)
        ]

        incident_not_updated_since_last_comment = len(comments) != 0

        if (
            incident_not_updated_since_creation
            or incident_not_updated_since_last_comment
        ):
            # if this issue is closed but we have not yet seen an update to it, let it
            # remain closed. this is supposed to capture the case where we have merged
            # a PR that used "Closes #issue" but we have not yet seen a successful import
            logging.info(
                "Found up-to-date issue [#%s] for an incident [%s] that is still ongoing.",
                issue.number,
                incident.unique_incident_id,
            )
        else:
            logging.info(
                "Found out-of-date issue [#%s] for an incident [%s] that is still ongoing. Updating...",
                issue.number,
                incident.unique_incident_id,
            )
            # now -- if the issue is closed, we have found a new failure so we need to
            # re-open it if is closed
            if issue.state == "closed":
                issue.edit(state="open")

            # add a our new comment
            issue.create_comment(body=self._format_incident_issue_comment(incident))

    def handle_incident(self, incident: AirflowAlertingIncident) -> None:

        existing_issue = self._search_past_issues_for_incident(incident)

        if incident.next_success_date is not None:
            self._ensure_issue_closed_for_resolved_incident(existing_issue, incident)
        elif not existing_issue:
            self._open_new_issue(incident)
        else:
            self._update_existing_issue_for_ongoing_incident(existing_issue, incident)
