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
"""Tests for the stale raw data GitHub alerting service."""

import datetime
from typing import Generator
from unittest.mock import Mock, create_autospec, patch

import pytest
from github.Repository import Repository

from recidiviz.airflow.dags.monitoring.stale_raw_data_alerting_incident import (
    StaleRawDataAlertingIncident,
)
from recidiviz.airflow.dags.monitoring.stale_raw_data_github_alerting_service import (
    StaleRawDataGitHubService,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCPEnvironment
from recidiviz.utils.github import HELPERBOT_USER_NAME


@pytest.fixture(name="github_mocks")
def fixture_github_mocks() -> Generator[Mock, None, None]:
    """Fixture that sets up GitHub and environment mocks for stale_raw_data_github_alerting_service tests."""
    github_client_patch = patch(
        "recidiviz.airflow.dags.monitoring.recidiviz_github_mixin.GithubHook.get_conn",
    )

    env_mock = patch(
        "recidiviz.airflow.dags.monitoring.stale_raw_data_github_alerting_service.get_environment_for_project",
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


def _create_comment(body: str, login: str = HELPERBOT_USER_NAME) -> Mock:
    """Helper function to create a mock comment."""
    m = Mock()
    m.body = body
    m.user.login = login
    return m


class TestStaleRawDataGitHubService:
    """Tests for StaleRawDataGitHubService"""

    def test_create_new_stale_data_issue(
        self,
        github_mocks: Mock,
    ) -> None:
        service = StaleRawDataGitHubService.get_stale_raw_data_service_for_state_code(
            project_id="recidiviz-staging", state_code=StateCode.US_XX
        )

        incident = StaleRawDataAlertingIncident(
            state_code="US_XX",
            file_tag="test_file",
            hours_stale=48.5,
            most_recent_import_date=datetime.datetime(2024, 1, 1),
        )

        mock_issue = _create_issue(
            title="[US_XX] [Staging] Stale raw data: test_file, last import: 2024-01-01T00:00:00",
            issue_id=99999,
        )
        github_mocks.create_issue.return_value = mock_issue
        github_mocks.get_issues.return_value = []

        service.handle_incident(incident)

        github_mocks.create_issue.assert_called_with(
            title="[US_XX] [Staging] Stale raw data: test_file, last import: 2024-01-01T00:00:00",
            body="Raw data file `test_file` for `US_XX` is **48.5 hours stale**.\n\n**Last successful import:** `2024-01-01 00:00:00`",
            labels=["Stale Raw Data", "Team: State Pod", "Region: US_XX", "Staging"],
        )

    def test_update_existing_stale_data_issue_reopen_and_create_comment(
        self,
        github_mocks: Mock,
    ) -> None:
        service = StaleRawDataGitHubService.get_stale_raw_data_service_for_state_code(
            project_id="recidiviz-staging", state_code=StateCode.US_XX
        )

        incident = StaleRawDataAlertingIncident(
            state_code="US_XX",
            file_tag="test_file",
            hours_stale=72.3,
            most_recent_import_date=datetime.datetime(2024, 1, 1),
        )

        existing_issue = _create_issue(
            title="[US_XX] [Staging] Stale raw data: test_file, last import: 2024-01-01T00:00:00",
            issue_id=54321,
            state="closed",
        )
        existing_issue.get_comments.return_value = []

        github_mocks.get_issues.return_value = [existing_issue]

        service.handle_incident(incident)

        existing_issue.edit.assert_called_with(state="open")
        existing_issue.create_comment.assert_called_with(
            "File is still stale: **72.3 hours** over threshold.\n\n**Last successful import:** `2024-01-01 00:00:00`"
        )

    def test_update_existing_stale_data_issue_edit_helperbot_comment(
        self,
        github_mocks: Mock,
    ) -> None:
        service = StaleRawDataGitHubService.get_stale_raw_data_service_for_state_code(
            project_id="recidiviz-staging", state_code=StateCode.US_XX
        )

        incident = StaleRawDataAlertingIncident(
            state_code="US_XX",
            file_tag="test_file",
            hours_stale=50.0,
            most_recent_import_date=datetime.datetime(2024, 1, 1),
        )

        existing_issue = _create_issue(
            title="[US_XX] [Staging] Stale raw data: test_file, last import: 2024-01-01T00:00:00",
            issue_id=54321,
            state="open",
        )

        helperbot_comment = _create_comment(
            "File is still stale: **40.0 hours** over threshold.\n\n**Last successful import:** `2024-01-01 00:00:00`"
        )
        existing_issue.get_comments.return_value = [helperbot_comment]

        github_mocks.get_issues.return_value = [existing_issue]

        service.handle_incident(incident)

        existing_issue.edit.assert_not_called()
        existing_issue.create_comment.assert_not_called()
        helperbot_comment.edit.assert_called_with(
            "File is still stale: **50.0 hours** over threshold.\n\n**Last successful import:** `2024-01-01 00:00:00`"
        )

    def test_update_existing_stale_data_issue_ignores_non_helperbot_comments(
        self,
        github_mocks: Mock,
    ) -> None:
        service = StaleRawDataGitHubService.get_stale_raw_data_service_for_state_code(
            project_id="recidiviz-staging", state_code=StateCode.US_XX
        )

        incident = StaleRawDataAlertingIncident(
            state_code="US_XX",
            file_tag="test_file",
            hours_stale=50.0,
            most_recent_import_date=datetime.datetime(2024, 1, 1),
        )

        existing_issue = _create_issue(
            title="[US_XX] [Staging] Stale raw data: test_file, last import: 2024-01-01T00:00:00",
            issue_id=54321,
            state="open",
        )

        non_helperbot_comment = _create_comment(
            "File is still stale: investigating.",
            login="some-other-user",
        )
        existing_issue.get_comments.return_value = [non_helperbot_comment]

        github_mocks.get_issues.return_value = [existing_issue]

        service.handle_incident(incident)

        existing_issue.edit.assert_not_called()
        non_helperbot_comment.edit.assert_not_called()
        existing_issue.create_comment.assert_called_with(
            "File is still stale: **50.0 hours** over threshold.\n\n**Last successful import:** `2024-01-01 00:00:00`"
        )

    def test_handle_resolved_incident_closes_all_matching_issues(
        self,
        github_mocks: Mock,
    ) -> None:
        service = StaleRawDataGitHubService.get_stale_raw_data_service_for_state_code(
            project_id="recidiviz-staging", state_code=StateCode.US_XX
        )

        incident = StaleRawDataAlertingIncident(
            state_code="US_XX",
            file_tag="test_file",
            hours_stale=-5.0,
            most_recent_import_date=datetime.datetime(2024, 1, 15),
        )

        old_issue_1 = _create_issue(
            title="[US_XX] [Staging] Stale raw data: test_file, last import: 2024-01-01",
            issue_id=11111,
            state="closed",
        )
        old_issue_2 = _create_issue(
            title="[US_XX] [Staging] Stale raw data: test_file, last import: 2024-01-10",
            issue_id=22222,
            state="open",
        )
        different_file_issue = _create_issue(
            title="[US_XX] [Staging] Stale raw data: other_file, last import: 2024-01-10",
            issue_id=33333,
            state="open",
        )

        github_mocks.get_issues.return_value = [
            old_issue_1,
            old_issue_2,
            different_file_issue,
        ]

        service.handle_incident(incident)

        old_issue_1.create_comment.assert_not_called()
        old_issue_1.edit.assert_not_called()

        old_issue_2.create_comment.assert_called_with(
            "File is now current. Last import: `2024-01-15 00:00:00`. Closing issue."
        )
        old_issue_2.edit.assert_called_with(state="closed", state_reason="completed")

        different_file_issue.create_comment.assert_not_called()
        different_file_issue.edit.assert_not_called()

    def test_handle_resolved_incident_no_open_issues(
        self,
        github_mocks: Mock,
    ) -> None:
        service = StaleRawDataGitHubService.get_stale_raw_data_service_for_state_code(
            project_id="recidiviz-staging", state_code=StateCode.US_XX
        )

        incident = StaleRawDataAlertingIncident(
            state_code="US_XX",
            file_tag="test_file",
            hours_stale=-5.0,
            most_recent_import_date=datetime.datetime(2024, 1, 15),
        )

        github_mocks.get_issues.return_value = []

        service.handle_incident(incident)

        github_mocks.create_issue.assert_not_called()
