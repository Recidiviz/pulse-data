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
from unittest.mock import ANY, create_autospec

from github.Issue import Issue
from github.Repository import Repository

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.tests.validation.validation_manager_test import (
    FakeValidationResultDetails,
)
from recidiviz.validation.checks.existence_check import ExistenceDataValidationCheck
from recidiviz.validation.validation_github_ticket_manager import (
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

    def _get_region_manager(self) -> "ValidationGithubTicketRegionManager":
        return ValidationGithubTicketRegionManager(
            client=self.client,
            env="staging",
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

        # existing
        manager.handle_result(existing_issues=dict(self._matching_fake_issue()))
        self.client.create_issue.assert_not_called()
        self.issue.create_comment.assert_called_with(
            "Found validation result for `test_1` with the status of [SUCCESS]."
        )
        self.issue.edit.assert_called_with(state="closed", state_reason="completed")

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

        # existing
        manager.handle_result(existing_issues=dict(self._matching_fake_issue()))
        self.client.create_issue.assert_not_called()
        self.issue.create_comment.assert_called_with(
            "Found validation result for `test_1` with the status of [FAIL_SOFT]."
        )
        self.issue.edit.assert_called_with(state="closed", state_reason="completed")

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

        # no existing
        manager.handle_result(existing_issues=dict(self._matching_fake_issue()))
        self.client.create_issue.assert_not_called()
        self.issue.create_comment.assert_not_called()
        self.issue.edit.assert_not_called()

        # existing
        manager.handle_result(existing_issues=dict(self._mismatching_fake_issue()))
        self.client.create_issue.assert_called_with(
            title=self.issue.title,
            body=ANY,
            labels=["Validation", "Team: State Pod", "Region: US_XX"],
        )
        self.not_the_issue.create_comment.assert_not_called()
        self.not_the_issue.edit.assert_not_called()
