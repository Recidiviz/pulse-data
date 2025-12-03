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
"""Tests for the Github Alerting service."""

import datetime
import logging
from typing import Generator
from unittest.mock import Mock, create_autospec, patch

import pytest
from _pytest.logging import LogCaptureFixture
from github.Repository import Repository

from recidiviz.airflow.dags.monitoring.airflow_alerting_incident import (
    AirflowAlertingIncident,
)
from recidiviz.airflow.dags.monitoring.recidiviz_github_alerting_service import (
    RecidivizGitHubService,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCPEnvironment

TEST_DAG = "test_dag"


@pytest.fixture(name="github_mocks")
def fixture_github_mocks() -> Generator[Mock, None, None]:
    """Fixture that sets up GitHub and environment mocks for recidiviz_github_alerting_service tests."""
    github_client_patch = patch(
        "recidiviz.airflow.dags.monitoring.recidiviz_github_alerting_service.GithubHook.get_conn",
    )

    env_mock = patch(
        "recidiviz.airflow.dags.monitoring.recidiviz_github_alerting_service.get_environment_for_project",
        return_value=GCPEnvironment.STAGING,
    )

    github_repo_mock = create_autospec(Repository)
    github_client_mock = github_client_patch.start()
    github_client_mock().get_repo.return_value = github_repo_mock

    env_mock.start()

    yield github_repo_mock

    github_client_patch.stop()
    env_mock.stop()


def _create_issue(title: str, state: str = "open", issue_id: int = 12345) -> Mock:
    """Helper function to create a mock issue."""
    m = Mock()
    m.title = title
    m.state = state
    m.number = issue_id
    return m


def _create_comment(body: str, login: str = "helperbot-recidiviz") -> Mock:
    """Helper function to create a mock comment."""
    m = Mock()
    m.body = body
    m.user.login = login
    return m


class TestRecidivizGitHubService:
    """Tests for RecidivizGitHubService"""

    def test_default_labels(
        self, github_mocks: Mock  # pylint: disable=unused-argument
    ) -> None:
        service = RecidivizGitHubService.raw_data_service_for_state_code(
            project_id="recidiviz-test", state_code=StateCode.US_XX
        )

        assert set(service.issue_labels) == {
            "Raw Data Import Failure",
            "Team: State Pod",
            "Region: US_XX",
            "Staging",
        }

    def test_search_past_incident_match_on_github_formatted(
        self, github_mocks: Mock
    ) -> None:
        service = RecidivizGitHubService.raw_data_service_for_state_code(
            project_id="recidiviz-test", state_code=StateCode.US_XX
        )

        mock_incident = AirflowAlertingIncident(
            dag_id="test_dag",
            dag_run_config="{}",
            job_id="a.job.id",
            failed_execution_dates=[
                datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC),
            ],
            previous_success_date=datetime.datetime(2023, 12, 31, tzinfo=datetime.UTC),
            incident_type="Task Run",
        )

        mock_issues = [
            _create_issue(
                "Task Run: {key: value}test_dag.a.job.id, started: 2024-01-01 00:00 UTC"
            ),
            _create_issue(
                "Task Run: test_dag.a.different.job.id, started: 2024-01-01 00:00 UTC"
            ),
        ]

        github_mocks.get_issues.return_value = mock_issues

        # pylint: disable=protected-access
        result = service._search_past_issues_for_incident(mock_incident)

        assert result is None

        github_mocks.get_issues.assert_called_with(
            sort="created",
            direction="desc",
            labels=[
                "Raw Data Import Failure",
                "Team: State Pod",
                "Region: US_XX",
                "Staging",
            ],
            creator="helperbot-recidiviz",
            state="all",
            since=datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.UTC),
        )

        mock_issues.append(
            _create_issue("[staging][US_XX] a.job.id, started: 2024-01-01 00:00 UTC")
        )

        github_mocks.get_issues.return_value = mock_issues

        # pylint: disable=protected-access
        result = service._search_past_issues_for_incident(mock_incident)

        github_mocks.get_issues.assert_called_with(
            sort="created",
            direction="desc",
            labels=[
                "Raw Data Import Failure",
                "Team: State Pod",
                "Region: US_XX",
                "Staging",
            ],
            creator="helperbot-recidiviz",
            state="all",
            since=datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.UTC),
        )

        assert result is not None
        assert (
            result.title == "[staging][US_XX] a.job.id, started: 2024-01-01 00:00 UTC"
        )
        # pylint: disable=protected-access
        assert result.title == service._get_issue_title_from_incident(mock_incident)

    def test_new_incident(self, github_mocks: Mock) -> None:
        service = RecidivizGitHubService.raw_data_service_for_state_code(
            project_id="recidiviz-testing", state_code=StateCode.US_XX
        )

        mock_incident = AirflowAlertingIncident(
            dag_id="test_dag",
            dag_run_config="{}",
            job_id="a.job.id",
            failed_execution_dates=[
                datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC),
            ],
            previous_success_date=datetime.datetime(2023, 12, 31, tzinfo=datetime.UTC),
            incident_type="Task Run",
        )

        mock_issues = [
            _create_issue(
                "Task Run: {key: value}test_dag.a.job.id, started: 2024-01-01 00:00 UTC"
            ),
            _create_issue(
                "Task Run: test_dag.a.different.job.id, started: 2024-01-01 00:00 UTC"
            ),
        ]

        github_mocks.get_issues.return_value = mock_issues

        service.handle_incident(mock_incident)

        github_mocks.get_issues.assert_called_with(
            sort="created",
            direction="desc",
            labels=[
                "Raw Data Import Failure",
                "Team: State Pod",
                "Region: US_XX",
                "Staging",
            ],
            creator="helperbot-recidiviz",
            state="all",
            since=datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.UTC),
        )

        github_mocks.create_issue.assert_called_with(
            title="[staging][US_XX] a.job.id, started: 2024-01-01 00:00 UTC",
            body="Failed run of [`a.job.id`] on the following dates: [ `2024-01-01T00:00:00+00:00`, `2024-01-02T00:00:00+00:00`, `2024-01-03T00:00:00+00:00` ].",
            labels=[
                "Raw Data Import Failure",
                "Team: State Pod",
                "Region: US_XX",
                "Staging",
            ],
        )

        mock_incident.error_message = "a.job has failed for the sake of testing ~~"

        service.handle_incident(mock_incident)

        github_mocks.get_issues.assert_called_with(
            sort="created",
            direction="desc",
            labels=[
                "Raw Data Import Failure",
                "Team: State Pod",
                "Region: US_XX",
                "Staging",
            ],
            creator="helperbot-recidiviz",
            state="all",
            since=datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.UTC),
        )

        github_mocks.create_issue.assert_called_with(
            title="[staging][US_XX] a.job.id, started: 2024-01-01 00:00 UTC",
            body="""Failed run of [`a.job.id`] on the following dates: [ `2024-01-01T00:00:00+00:00`, `2024-01-02T00:00:00+00:00`, `2024-01-03T00:00:00+00:00` ].\n<details>\n<summary>Most recent error message:</summary>\n\n```\na.job has failed for the sake of testing ~~\n```\n</details>""",
            labels=[
                "Raw Data Import Failure",
                "Team: State Pod",
                "Region: US_XX",
                "Staging",
            ],
        )

    def test_existing_incident_ongoing_no_update_needed_description(
        self, github_mocks: Mock, caplog: LogCaptureFixture
    ) -> None:
        service = RecidivizGitHubService.raw_data_service_for_state_code(
            project_id="recidiviz-123", state_code=StateCode.US_XX
        )

        mock_incident = AirflowAlertingIncident(
            dag_id="test_dag",
            dag_run_config="{}",
            job_id="a.job.id",
            failed_execution_dates=[datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)],
            previous_success_date=datetime.datetime(2023, 12, 31, tzinfo=datetime.UTC),
            incident_type="Task Run",
        )

        issue = _create_issue(
            "[staging][US_XX] a.job.id, started: 2024-01-01 00:00 UTC", issue_id=54321
        )

        mock_issues = [
            _create_issue(
                "Task Run: {key: value}test_dag.a.job.id, started: 2024-01-01 00:00 UTC"
            ),
            _create_issue(
                "Task Run: test_dag.a.different.job.id, started: 2024-01-01 00:00 UTC"
            ),
            issue,
        ]

        issue.body = "Failed run of [`a.job.id`] on the following dates: [ `2024-01-01T00:00:00+00:00` ]. Here is some more text too to make sure we are doing startswith."
        issue.get_comments.return_value = []

        github_mocks.get_issues.return_value = mock_issues

        with caplog.at_level(
            logging.INFO,
        ):
            service.handle_incident(mock_incident)

        github_mocks.get_issues.assert_called_with(
            sort="created",
            direction="desc",
            labels=[
                "Raw Data Import Failure",
                "Team: State Pod",
                "Region: US_XX",
                "Staging",
            ],
            creator="helperbot-recidiviz",
            state="all",
            since=datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.UTC),
        )

        github_mocks.create_issue.assert_not_called()
        issue.edit.assert_not_called()
        issue.create_comment.assert_not_called()

        assert len(caplog.records) == 1
        assert (
            caplog.records[0].message
            == "Found up-to-date issue [#54321] for an incident [Task Run: test_dag.a.job.id, started: 2024-01-01 00:00 UTC] that is still ongoing."
        )

    def test_existing_incident_ongoing_no_update_needed_comment(
        self, github_mocks: Mock, caplog: LogCaptureFixture
    ) -> None:
        service = RecidivizGitHubService.raw_data_service_for_state_code(
            project_id="recidiviz-staging", state_code=StateCode.US_XX
        )

        mock_incident = AirflowAlertingIncident(
            dag_id="test_dag",
            dag_run_config="{}",
            job_id="a.job.id",
            failed_execution_dates=[
                datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
            ],
            previous_success_date=datetime.datetime(2023, 12, 31, tzinfo=datetime.UTC),
            incident_type="Task Run",
        )

        issue = _create_issue(
            "[staging][US_XX] a.job.id, started: 2024-01-01 00:00 UTC", issue_id=54321
        )

        mock_issues = [
            _create_issue(
                "Task Run: {key: value}test_dag.a.job.id, started: 2024-01-01 00:00 UTC"
            ),
            _create_issue(
                "Task Run: test_dag.a.different.job.id, started: 2024-01-01 00:00 UTC"
            ),
            issue,
        ]

        issue.body = "Failed run of [`a.job.id`] on the following dates: [ `2024-01-01T00:00:00+00:00` ]. Here is some more text too to make sure we are doing startswith."
        issue.get_comments.return_value = [
            _create_comment(
                "Failure for [`a.job.id`] on [ `2024-01-02T00:00:00+00:00` ]. Here is some more text too to make sure we are doing startswith."
            ),
        ]

        github_mocks.get_issues.return_value = mock_issues

        with caplog.at_level(
            logging.INFO,
        ):
            service.handle_incident(mock_incident)

        github_mocks.get_issues.assert_called_with(
            sort="created",
            direction="desc",
            labels=[
                "Raw Data Import Failure",
                "Team: State Pod",
                "Region: US_XX",
                "Staging",
            ],
            creator="helperbot-recidiviz",
            state="all",
            since=datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.UTC),
        )

        github_mocks.create_issue.assert_not_called()
        issue.edit.assert_not_called()
        issue.create_comment.assert_not_called()

        assert len(caplog.records) == 1
        assert (
            caplog.records[0].message
            == "Found up-to-date issue [#54321] for an incident [Task Run: test_dag.a.job.id, started: 2024-01-01 00:00 UTC] that is still ongoing."
        )

    def test_existing_incident_ongoing_update_and_reopen(
        self, github_mocks: Mock, caplog: LogCaptureFixture
    ) -> None:
        service = RecidivizGitHubService.raw_data_service_for_state_code(
            project_id="recidiviz-test", state_code=StateCode.US_XX
        )

        mock_incident = AirflowAlertingIncident(
            dag_id="test_dag",
            dag_run_config="{}",
            job_id="a.job.id",
            failed_execution_dates=[
                datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC),
            ],
            previous_success_date=datetime.datetime(2023, 12, 31, tzinfo=datetime.UTC),
            incident_type="Task Run",
        )

        issue = _create_issue(
            "[staging][US_XX] a.job.id, started: 2024-01-01 00:00 UTC",
            issue_id=54321,
            state="closed",
        )

        mock_issues = [
            _create_issue(
                "Task Run: {key: value}test_dag.a.job.id, started: 2024-01-01 00:00 UTC"
            ),
            _create_issue(
                "Task Run: test_dag.a.different.job.id, started: 2024-01-01 00:00 UTC"
            ),
            issue,
        ]

        issue.body = "Failed run of [`a.job.id`] on the following dates: [ `2024-01-01T00:00:00+00:00` ]. Here is some more text too to make sure we are doing startswith."
        issue.get_comments.return_value = [
            _create_comment(
                "Failure for [`a.job.id`] on [ `2024-01-02T00:00:00+00:00` ]. Here is some more text too to make sure we are doing startswith."
            ),
        ]

        github_mocks.get_issues.return_value = mock_issues

        with caplog.at_level(
            logging.INFO,
        ):
            service.handle_incident(mock_incident)

        github_mocks.get_issues.assert_called_with(
            sort="created",
            direction="desc",
            labels=[
                "Raw Data Import Failure",
                "Team: State Pod",
                "Region: US_XX",
                "Staging",
            ],
            creator="helperbot-recidiviz",
            state="all",
            since=datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.UTC),
        )

        github_mocks.create_issue.assert_not_called()
        issue.edit.assert_called_with(state="open")
        issue.create_comment.assert_called_with(
            body="Failure for [`a.job.id`] on [ `2024-01-03T00:00:00+00:00` ]."
        )

        assert len(caplog.records) == 1
        assert (
            caplog.records[0].message
            == "Found out-of-date issue [#54321] for an incident [Task Run: test_dag.a.job.id, started: 2024-01-01 00:00 UTC] that is still ongoing. Updating..."
        )

    def test_existing_incident_ongoing_update(
        self, github_mocks: Mock, caplog: LogCaptureFixture
    ) -> None:
        service = RecidivizGitHubService.raw_data_service_for_state_code(
            project_id="recidiviz-staging", state_code=StateCode.US_XX
        )

        mock_incident = AirflowAlertingIncident(
            dag_id="test_dag",
            dag_run_config="{}",
            job_id="a.job.id",
            failed_execution_dates=[
                datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC),
            ],
            previous_success_date=datetime.datetime(2023, 12, 31, tzinfo=datetime.UTC),
            incident_type="Task Run",
        )

        issue = _create_issue(
            "[staging][US_XX] a.job.id, started: 2024-01-01 00:00 UTC",
            issue_id=54321,
        )

        mock_issues = [
            _create_issue(
                "Task Run: {key: value}test_dag.a.job.id, started: 2024-01-01 00:00 UTC"
            ),
            _create_issue(
                "Task Run: test_dag.a.different.job.id, started: 2024-01-01 00:00 UTC"
            ),
            issue,
        ]

        issue.body = "Failed run of [`a.job.id`] on the following dates: [ `2024-01-01T00:00:00+00:00` ]. Here is some more text too to make sure we are doing startswith."
        issue.get_comments.return_value = [
            _create_comment(
                "Failure for [a.job.id] on [ 2024-01-02T00:00:00+00:00 ]. Here is some more text too to make sure we are doing startswith."
            ),
            _create_comment(
                "Failure for [`a.job.id`] on [ `2024-01-03T00:00:00+00:00` ].",
                login="not-helper-bot",
            ),
        ]

        github_mocks.get_issues.return_value = mock_issues

        with caplog.at_level(
            logging.INFO,
        ):
            service.handle_incident(mock_incident)

        github_mocks.get_issues.assert_called_with(
            sort="created",
            direction="desc",
            labels=[
                "Raw Data Import Failure",
                "Team: State Pod",
                "Region: US_XX",
                "Staging",
            ],
            creator="helperbot-recidiviz",
            state="all",
            since=datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.UTC),
        )

        github_mocks.create_issue.assert_not_called()
        issue.edit.assert_not_called()
        issue.create_comment.assert_called_with(
            body="Failure for [`a.job.id`] on [ `2024-01-03T00:00:00+00:00` ]."
        )

        assert len(caplog.records) == 1
        assert (
            caplog.records[0].message
            == "Found out-of-date issue [#54321] for an incident [Task Run: test_dag.a.job.id, started: 2024-01-01 00:00 UTC] that is still ongoing. Updating..."
        )

    def test_existing_issue_resolved_and_open(
        self, github_mocks: Mock, caplog: LogCaptureFixture
    ) -> None:
        service = RecidivizGitHubService.raw_data_service_for_state_code(
            project_id="recidiviz-staging", state_code=StateCode.US_XX
        )

        mock_incident = AirflowAlertingIncident(
            dag_id="test_dag",
            dag_run_config="{}",
            job_id="a.job.id",
            failed_execution_dates=[
                datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC),
            ],
            previous_success_date=datetime.datetime(2023, 12, 31, tzinfo=datetime.UTC),
            next_success_date=datetime.datetime(2024, 1, 4, tzinfo=datetime.UTC),
            incident_type="Task Run",
        )

        incident_issue = _create_issue(
            "[staging][US_XX] a.job.id, started: 2024-01-01 00:00 UTC",
            issue_id=54321,
        )

        mock_issues = [
            _create_issue(
                "Task Run: {key: value}test_dag.a.job.id, started: 2024-01-01 00:00 UTC"
            ),
            _create_issue(
                "Task Run: test_dag.a.different.job.id, started: 2024-01-01 00:00 UTC"
            ),
            incident_issue,
        ]

        github_mocks.get_issues.return_value = mock_issues

        with caplog.at_level(
            logging.INFO,
        ):
            service.handle_incident(mock_incident)

        github_mocks.get_issues.assert_called_with(
            sort="created",
            direction="desc",
            labels=[
                "Raw Data Import Failure",
                "Team: State Pod",
                "Region: US_XX",
                "Staging",
            ],
            creator="helperbot-recidiviz",
            state="all",
            since=datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.UTC),
        )

        github_mocks.create_issue.assert_not_called()

        incident_issue.create_comment.assert_called_with(
            "Successful job completion found on [`2024-01-04T00:00:00+00:00`]; closing issue."
        )
        incident_issue.edit.assert_called_with(state="closed", state_reason="completed")

        assert len(caplog.records) == 1
        assert (
            caplog.records[0].message
            == "Closed issue [#54321] for incident [Task Run: test_dag.a.job.id, started: 2024-01-01 00:00 UTC] as a job run completed successfully on [2024-01-04T00:00:00+00:00]"
        )

    def test_existing_issue_resolved_and_already_closed(
        self, github_mocks: Mock, caplog: LogCaptureFixture
    ) -> None:
        service = RecidivizGitHubService.raw_data_service_for_state_code(
            project_id="recidiviz-staging", state_code=StateCode.US_XX
        )

        mock_incident = AirflowAlertingIncident(
            dag_id="test_dag",
            dag_run_config="{}",
            job_id="a.job.id",
            failed_execution_dates=[
                datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC),
            ],
            previous_success_date=datetime.datetime(2023, 12, 31, tzinfo=datetime.UTC),
            next_success_date=datetime.datetime(2024, 1, 4, tzinfo=datetime.UTC),
            incident_type="Task Run",
        )

        incident_issue = _create_issue(
            "[staging][US_XX] a.job.id, started: 2024-01-01 00:00 UTC",
            issue_id=54321,
            state="closed",
        )

        mock_issues = [
            _create_issue(
                "Task Run: {key: value}test_dag.a.job.id, started: 2024-01-01 00:00 UTC"
            ),
            _create_issue(
                "Task Run: test_dag.a.different.job.id, started: 2024-01-01 00:00 UTC"
            ),
            incident_issue,
        ]

        github_mocks.get_issues.return_value = mock_issues

        with caplog.at_level(
            logging.INFO,
        ):
            service.handle_incident(mock_incident)

        github_mocks.get_issues.assert_called_with(
            sort="created",
            direction="desc",
            labels=[
                "Raw Data Import Failure",
                "Team: State Pod",
                "Region: US_XX",
                "Staging",
            ],
            creator="helperbot-recidiviz",
            state="all",
            since=datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.UTC),
        )

        github_mocks.create_issue.assert_not_called()

        incident_issue.create_comment.assert_not_called()
        incident_issue.edit.assert_not_called()

        assert len(caplog.records) == 1
        assert caplog.records[0].message == "Issue doesn't exist or is already closed"

    def test_existing_issue_resolved_and_never_opened(
        self, github_mocks: Mock, caplog: LogCaptureFixture
    ) -> None:
        service = RecidivizGitHubService.raw_data_service_for_state_code(
            project_id="recidiviz-staging", state_code=StateCode.US_XX
        )

        mock_incident = AirflowAlertingIncident(
            dag_id="test_dag",
            dag_run_config="{}",
            job_id="a.job.id",
            failed_execution_dates=[
                datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
                datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC),
            ],
            previous_success_date=datetime.datetime(2023, 12, 31, tzinfo=datetime.UTC),
            next_success_date=datetime.datetime(2024, 1, 4, tzinfo=datetime.UTC),
            incident_type="Task Run",
        )

        mock_issues = [
            _create_issue(
                "Task Run: {key: value}test_dag.a.job.id, started: 2024-01-01 00:00 UTC"
            ),
            _create_issue(
                "Task Run: test_dag.a.different.job.id, started: 2024-01-01 00:00 UTC"
            ),
        ]

        github_mocks.get_issues.return_value = mock_issues

        with caplog.at_level(
            logging.INFO,
        ):
            service.handle_incident(mock_incident)

        github_mocks.get_issues.assert_called_with(
            sort="created",
            direction="desc",
            labels=[
                "Raw Data Import Failure",
                "Team: State Pod",
                "Region: US_XX",
                "Staging",
            ],
            creator="helperbot-recidiviz",
            state="all",
            since=datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.UTC),
        )

        github_mocks.create_issue.assert_not_called()

        assert len(caplog.records) == 1
        assert caplog.records[0].message == "Issue doesn't exist or is already closed"
