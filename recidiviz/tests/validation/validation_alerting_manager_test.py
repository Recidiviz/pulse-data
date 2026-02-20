# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Unit tests for github ticket manager classes"""
from unittest import TestCase
from unittest.mock import ANY, MagicMock, create_autospec, patch

from github.Issue import Issue
from github.Repository import Repository

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.tests.validation.validation_manager_test import (
    FakeValidationResultDetails,
)
from recidiviz.utils.environment import GCPEnvironment
from recidiviz.validation.checks.existence_check import ExistenceDataValidationCheck
from recidiviz.validation.validation_alerting_manager import (
    ValidationGithubTicketManager,
    ValidationGithubTicketRegionManager,
)
from recidiviz.validation.validation_models import (
    DataValidationJob,
    DataValidationJobResult,
    ValidationCategory,
    ValidationResultStatus,
)


class ValidationGithubTicketManagerTest(TestCase):
    """Unit tests for github ticket manager class"""

    validation_job: DataValidationJob

    @classmethod
    def setUpClass(cls) -> None:
        cls.validation_job = DataValidationJob(
            region_code="US_XX",
            validation=ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_1",
                    description="test_1 description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )

    def setUp(self) -> None:
        self.client = create_autospec(Repository)
        self.issue = create_autospec(Issue)
        self.issue.title = "[staging][US_XX] `test_1`"
        self.not_the_issue = create_autospec(Issue)
        self.not_the_issue.title = "[staging][US_XX] `test_2`"

        assignees_patcher = patch(
            "recidiviz.validation.validation_alerting_manager._get_default_github_assignees_for_state"
        )
        self.mock_get_assignees = assignees_patcher.start()
        self.mock_get_assignees.return_value = ["test_github_handle"]
        self.addCleanup(assignees_patcher.stop)

    def _get_region_manager(self) -> "ValidationGithubTicketRegionManager":
        return ValidationGithubTicketRegionManager(
            client=self.client,
            env=GCPEnvironment.STAGING,
            state_code=StateCode.US_XX,
        )

    def _matching_fake_issue(self) -> dict[str, Issue]:
        return {self.issue.title: self.issue}

    def _mismatching_fake_issue(self) -> dict[str, Issue]:
        return {self.not_the_issue.title: self.not_the_issue}

    def test_successful_validation(self) -> None:
        successful_validation = DataValidationJobResult(
            validation_job=self.validation_job,
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.SUCCESS
            ),
        )

        manager = ValidationGithubTicketManager.from_validation_result(
            self._get_region_manager(), successful_validation
        )

        # no existing
        manager.handle_result(existing_issues=dict(self._mismatching_fake_issue()))
        self.client.create_issue.assert_not_called()
        self.not_the_issue.create_comment.assert_not_called()
        self.not_the_issue.edit.assert_not_called()
        self.mock_get_assignees.assert_not_called()

        # existing
        manager.handle_result(existing_issues=dict(self._matching_fake_issue()))
        self.client.create_issue.assert_not_called()
        self.issue.create_comment.assert_called_with(
            "Found validation result for `test_1` with the status of [SUCCESS]."
        )
        self.issue.edit.assert_called_with(state="closed", state_reason="completed")
        self.mock_get_assignees.assert_not_called()

    def test_fail_soft_validation(self) -> None:
        successful_validation = DataValidationJobResult(
            validation_job=self.validation_job,
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_SOFT
            ),
        )

        manager = ValidationGithubTicketManager.from_validation_result(
            self._get_region_manager(), successful_validation
        )

        # no existing
        manager.handle_result(existing_issues=dict(self._mismatching_fake_issue()))
        self.client.create_issue.assert_not_called()
        self.not_the_issue.create_comment.assert_not_called()
        self.not_the_issue.edit.assert_not_called()
        self.mock_get_assignees.assert_not_called()

        # existing
        manager.handle_result(existing_issues=dict(self._matching_fake_issue()))
        self.client.create_issue.assert_not_called()
        self.issue.create_comment.assert_called_with(
            "Found validation result for `test_1` with the status of [FAIL_SOFT]."
        )
        self.issue.edit.assert_called_with(state="closed", state_reason="completed")
        self.mock_get_assignees.assert_not_called()

    def test_fail_hard_validation(self) -> None:
        successful_validation = DataValidationJobResult(
            validation_job=self.validation_job,
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD
            ),
        )

        manager = ValidationGithubTicketManager.from_validation_result(
            self._get_region_manager(), successful_validation
        )

        # no existing — issue already exists, so no new ticket is created
        manager.handle_result(existing_issues=dict(self._matching_fake_issue()))
        self.client.create_issue.assert_not_called()
        self.issue.create_comment.assert_not_called()
        self.issue.edit.assert_not_called()
        self.mock_get_assignees.assert_not_called()

        # existing — no matching issue, so a new ticket is created with assignees
        manager.handle_result(existing_issues=dict(self._mismatching_fake_issue()))
        self.mock_get_assignees.assert_called_once_with(StateCode.US_XX)
        self.client.create_issue.assert_called_with(
            title=self.issue.title,
            body=ANY,
            labels=["Validation", "Team: State Pod", "Region: US_XX"],
            assignees=["test_github_handle"],
        )
        self.not_the_issue.create_comment.assert_not_called()
        self.not_the_issue.edit.assert_not_called()

    @patch("recidiviz.validation.validation_alerting_manager.requests.post")
    @patch("recidiviz.validation.validation_alerting_manager.get_secret")
    def test_handle_results_posts_to_slack(
        self,
        mock_get_secret: MagicMock,
        mock_post: MagicMock,
    ) -> None:
        """Test that handle_results posts a batched Slack message for all created issues."""
        self.mock_get_assignees.return_value = []
        mock_get_secret.return_value = "fake-slack-token"

        # Setup mock for created issue
        mock_created_issue = MagicMock()
        mock_created_issue.html_url = "https://github.com/org/repo/issues/123"
        self.client.create_issue.return_value = mock_created_issue
        self.client.get_issues.return_value = []  # No existing issues

        fail_hard_validation = DataValidationJobResult(
            validation_job=self.validation_job,
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD
            ),
        )

        region_manager = self._get_region_manager()
        region_manager.handle_results(results=iter([fail_hard_validation]))

        # Verify GitHub issue was created
        self.client.create_issue.assert_called_once()

        # Verify Slack was called with correct parameters
        mock_post.assert_called_once_with(
            "https://slack.com/api/chat.postMessage",
            json={
                "text": ANY,
                "channel": "C09VDD8G89E",  # Default channel from YAML config
            },
            headers={"Authorization": "Bearer fake-slack-token"},
            timeout=60,
        )

        # Verify the Slack message content
        slack_call_args = mock_post.call_args
        slack_text = slack_call_args.kwargs["json"]["text"]
        self.assertIn("US_XX", slack_text)
        self.assertIn("test_1", slack_text)
        self.assertIn("staging", slack_text)
        self.assertIn("https://github.com/org/repo/issues/123", slack_text)
