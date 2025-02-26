# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Tests for validation/validation_manager.py."""
import json
from typing import List, Optional, Set
from unittest import TestCase
from unittest.mock import call

import attr
import mock
from flask import Flask
from github.Issue import Issue
from mock import MagicMock, patch

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.utils.matchers import UnorderedCollection
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCPEnvironment
from recidiviz.validation.checks.existence_check import ExistenceDataValidationCheck
from recidiviz.validation.checks.sameness_check import (
    SamenessDataValidationCheck,
    SamenessDataValidationCheckType,
)
from recidiviz.validation.configured_validations import (
    get_all_validations,
    get_validation_global_config,
    get_validation_region_configs,
)
from recidiviz.validation.validation_config import (
    ValidationMaxAllowedErrorOverride,
    ValidationNumAllowedRowsOverride,
    ValidationRegionConfig,
)
from recidiviz.validation.validation_manager import (
    _fetch_validation_jobs_to_perform,
    execute_validation,
    execute_validation_request,
    validation_manager_blueprint,
)
from recidiviz.validation.validation_models import (
    DataValidationJob,
    DataValidationJobResult,
    DataValidationJobResultDetails,
    ValidationCategory,
    ValidationCheckType,
    ValidationResultStatus,
)
from recidiviz.validation.views import view_config as validation_view_config


def get_test_validations() -> List[DataValidationJob]:
    return [
        DataValidationJob(
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
        ),
        DataValidationJob(
            region_code="US_XX",
            validation=ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_2",
                    description="test_2 description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        ),
        DataValidationJob(
            region_code="US_XX",
            validation=ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_3",
                    description="test_3 description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        ),
        DataValidationJob(
            region_code="US_XX",
            validation=ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_4",
                    description="test_4 description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        ),
        DataValidationJob(
            region_code="US_XX",
            validation=ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_5",
                    description="test_5 description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        ),
    ]


@attr.s(frozen=True, kw_only=True)
class FakeValidationResultDetails(DataValidationJobResultDetails):
    """Fake implementation of DataValidationJobResultDetails"""

    validation_status: ValidationResultStatus = attr.ib()
    dev_mode: bool = attr.ib(default=False)

    @property
    def has_data(self) -> bool:
        return True

    @property
    def is_dev_mode(self) -> bool:
        return self.dev_mode

    @property
    def error_amount(self) -> float:
        validation_result_status = self.validation_result_status()
        if validation_result_status == ValidationResultStatus.FAIL_SOFT:
            return 0.2
        if validation_result_status == ValidationResultStatus.FAIL_HARD:
            return 0.3
        return 0

    @property
    def hard_failure_amount(self) -> float:
        return 0.02

    @property
    def soft_failure_amount(self) -> float:
        return 0.01

    @property
    def error_is_percentage(self) -> bool:
        return True

    def validation_result_status(self) -> ValidationResultStatus:
        return self.validation_status

    def failure_description(self) -> Optional[str]:
        validation_result_status = self.validation_result_status()
        if validation_result_status == ValidationResultStatus.SUCCESS:
            return None
        if validation_result_status == ValidationResultStatus.FAIL_SOFT:
            return "FAIL SOFT"
        if validation_result_status == ValidationResultStatus.FAIL_HARD:
            return "FAIL HARD"
        raise AttributeError(
            f"failure_description for validation_result_status {validation_result_status} not set"
        )


APP_ENGINE_HEADERS = {
    "X-AppEngine-TaskName": "my-task-id",
    "X-Appengine-Cron": "test-cron",
}


class TestHandleRequest(TestCase):
    """Tests for the Validation Manager API endpoint."""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = GCP_PROJECT_STAGING
        self.project_number_patcher = patch("recidiviz.utils.metadata.project_number")
        self.project_number_patcher.start().return_value = "123456789"
        self.environment_patcher = patch(
            "recidiviz.validation.validation_manager.get_environment_for_project"
        )
        self.environment_patcher.start().return_value = GCPEnvironment.STAGING

        app = Flask(__name__)
        app.register_blueprint(validation_manager_blueprint)
        app.config["TESTING"] = True
        self.client = app.test_client()

        self._TEST_VALIDATIONS = get_test_validations()

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.project_number_patcher.stop()
        self.environment_patcher.stop()

    @patch(
        "recidiviz.validation.validation_manager._file_tickets_for_failing_validations"
    )
    @patch("recidiviz.validation.validation_manager._emit_opencensus_failure_events")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    @patch(
        "recidiviz.validation.validation_manager.store_validation_results_in_big_query"
    )
    @patch(
        "recidiviz.validation.validation_manager.store_validation_run_completion_in_big_query"
    )
    def test_handle_request_happy_path_no_failures(
        self,
        mock_store_run_success: MagicMock,
        mock_store_validation_results: MagicMock,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_opencensus_failure_events: MagicMock,
        mock_file_tickets_for_failing_validations: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS
        mock_run_job.return_value = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[0],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.SUCCESS
            ),
        )

        data = {"ingest_instance": "PRIMARY"}
        response = self.client.post(
            "/validate/US_XX", headers=APP_ENGINE_HEADERS, data=json.dumps(data)
        )

        self.assertEqual(200, response.status_code)

        self.assertEqual(5, mock_run_job.call_count)
        for job in self._TEST_VALIDATIONS:
            mock_run_job.assert_any_call(job)

        mock_emit_opencensus_failure_events.assert_not_called()
        mock_file_tickets_for_failing_validations.assert_not_called()
        mock_store_validation_results.assert_called_once()
        ((results,), _kwargs) = mock_store_validation_results.call_args
        self.assertEqual(5, len(results))

        mock_store_run_success.assert_called_with(
            cloud_task_id="my-task-id",
            num_validations_run=5,
            validations_runtime_sec=mock.ANY,
            validation_run_id=mock.ANY,
            ingest_instance=DirectIngestInstance.PRIMARY,
            sandbox_dataset_prefix=None,
        )

    @patch(
        "recidiviz.validation.validation_manager._file_tickets_for_failing_validations"
    )
    @patch("recidiviz.validation.validation_manager._emit_opencensus_failure_events")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    @patch(
        "recidiviz.validation.validation_manager.store_validation_results_in_big_query"
    )
    @patch(
        "recidiviz.validation.validation_manager.store_validation_run_completion_in_big_query"
    )
    def test_handle_request_happy_path_no_failures_secondary_with_sandbox_prefix(
        self,
        mock_store_run_success: MagicMock,
        mock_store_validation_results: MagicMock,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_opencensus_failure_events: MagicMock,
        mock_file_tickets_for_failing_validations: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS
        mock_run_job.return_value = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[0],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.SUCCESS
            ),
        )

        data = {"ingest_instance": "SECONDARY", "sandbox_prefix": "test_prefix"}
        response = self.client.post(
            "/validate/US_XX", headers=APP_ENGINE_HEADERS, data=json.dumps(data)
        )

        self.assertEqual(200, response.status_code)

        self.assertEqual(5, mock_run_job.call_count)
        for job in self._TEST_VALIDATIONS:
            mock_run_job.assert_any_call(job)

        mock_emit_opencensus_failure_events.assert_not_called()
        mock_file_tickets_for_failing_validations.assert_not_called()
        mock_store_validation_results.assert_called_once()
        ((results,), _kwargs) = mock_store_validation_results.call_args
        self.assertEqual(5, len(results))

        mock_store_run_success.assert_called_with(
            cloud_task_id="my-task-id",
            num_validations_run=5,
            validations_runtime_sec=mock.ANY,
            validation_run_id=mock.ANY,
            ingest_instance=DirectIngestInstance.SECONDARY,
            sandbox_dataset_prefix="test_prefix",
        )

    def test_handle_request_should_failure_secondary_with_no_sandbox_prefix(
        self,
    ) -> None:
        data = {"ingest_instance": "SECONDARY"}
        response = self.client.post(
            "/validate/US_XX", headers=APP_ENGINE_HEADERS, data=json.dumps(data)
        )

        self.assertEqual(400, response.status_code)
        self.assertEqual(
            response.get_data().decode(),
            "Sandbox prefix must be specified for secondary ingest instance",
        )

    @patch("recidiviz.validation.validation_manager.github_helperbot_client")
    @patch("recidiviz.validation.validation_manager._emit_opencensus_failure_events")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    @patch(
        "recidiviz.validation.validation_manager.store_validation_results_in_big_query"
    )
    @patch(
        "recidiviz.validation.validation_manager.store_validation_run_completion_in_big_query"
    )
    def test_handle_request_with_job_failures_and_validation_failures(
        self,
        mock_store_run_success: MagicMock,
        mock_store_validation_results: MagicMock,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_opencensus_failure_events: MagicMock,
        mock_github_client: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS
        mock_github_repo = MagicMock()
        mock_github_repo.get_issues.return_value = [
            Issue(
                requester=MagicMock(),
                headers=MagicMock(),
                attributes={"title": "[staging][US_XX] `test_3`"},
                completed=MagicMock(),
            )
        ]
        mock_github_client.return_value.get_repo.return_value = mock_github_repo
        first_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[1],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD
            ),
        )
        second_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[2],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD
            ),
        )
        third_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[3],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD, dev_mode=True
            ),
        )
        mock_run_job.side_effect = [
            DataValidationJobResult(
                validation_job=self._TEST_VALIDATIONS[0],
                result_details=FakeValidationResultDetails(
                    validation_status=ValidationResultStatus.SUCCESS
                ),
            ),
            first_failure,
            second_failure,
            third_failure,
            ValueError("Job failed to run!"),
        ]
        data = {"ingest_instance": "PRIMARY"}
        response = self.client.post(
            "/validate/US_XX", headers=APP_ENGINE_HEADERS, data=json.dumps(data)
        )

        self.assertEqual(200, response.status_code)

        self.assertEqual(len(self._TEST_VALIDATIONS), mock_run_job.call_count)
        for job in self._TEST_VALIDATIONS:
            mock_run_job.assert_any_call(job)

        mock_emit_opencensus_failure_events.assert_called_with(
            UnorderedCollection([self._TEST_VALIDATIONS[4]]),
            UnorderedCollection([first_failure, second_failure]),
        )
        mock_store_validation_results.assert_called_once()
        ((results,), _kwargs) = mock_store_validation_results.call_args
        self.assertEqual(5, len(results))

        mock_store_run_success.assert_called_with(
            cloud_task_id="my-task-id",
            num_validations_run=5,
            validations_runtime_sec=mock.ANY,
            validation_run_id=mock.ANY,
            ingest_instance=DirectIngestInstance.PRIMARY,
            sandbox_dataset_prefix=None,
        )

        expected_labels = ["Validation", "Region: US_XX", "Team: State Pod"]
        expected_calls = [
            call(
                title="[staging][US_XX] `test_2`",
                body=mock.ANY,
                labels=expected_labels,
            ),
            call(
                title="[staging][US_XX] `test_4`",
                body=mock.ANY,
                labels=expected_labels,
            ),
        ]
        self.assertCountEqual(
            mock_github_repo.create_issue.call_args_list, expected_calls
        )

    @patch("recidiviz.validation.validation_manager.github_helperbot_client")
    @patch("recidiviz.validation.validation_manager._emit_opencensus_failure_events")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    @patch(
        "recidiviz.validation.validation_manager.store_validation_results_in_big_query"
    )
    @patch(
        "recidiviz.validation.validation_manager.store_validation_run_completion_in_big_query"
    )
    def test_handle_request_happy_path_some_failures(
        self,
        mock_store_run_success: MagicMock,
        mock_store_validation_results: MagicMock,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_opencensus_failure_events: MagicMock,
        mock_github_client: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS
        mock_github_repo = MagicMock()
        mock_github_repo.get_issues.return_value = [
            Issue(
                requester=MagicMock(),
                headers=MagicMock(),
                attributes={"title": "[staging][US_XX] `test_3`"},
                completed=MagicMock(),
            )
        ]
        mock_github_client.return_value.get_repo.return_value = mock_github_repo

        first_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[1],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD
            ),
        )
        second_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[2],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD
            ),
        )
        third_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[3],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD, dev_mode=True
            ),
        )
        mock_run_job.side_effect = [
            DataValidationJobResult(
                validation_job=self._TEST_VALIDATIONS[0],
                result_details=FakeValidationResultDetails(
                    validation_status=ValidationResultStatus.SUCCESS
                ),
            ),
            first_failure,
            second_failure,
            third_failure,
            DataValidationJobResult(
                validation_job=self._TEST_VALIDATIONS[4],
                result_details=FakeValidationResultDetails(
                    validation_status=ValidationResultStatus.SUCCESS
                ),
            ),
        ]

        data = {"ingest_instance": "PRIMARY"}
        response = self.client.post(
            "/validate/US_XX", headers=APP_ENGINE_HEADERS, data=json.dumps(data)
        )

        self.assertEqual(200, response.status_code)

        self.assertEqual(5, mock_run_job.call_count)
        for job in self._TEST_VALIDATIONS:
            mock_run_job.assert_any_call(job)

        mock_emit_opencensus_failure_events.assert_called_with(
            [], UnorderedCollection([first_failure, second_failure])
        )
        mock_store_validation_results.assert_called_once()
        self.assertEqual(1, len(mock_store_validation_results.call_args[0]))
        ((results,), _kwargs) = mock_store_validation_results.call_args
        self.assertEqual(5, len(results))

        mock_store_run_success.assert_called_with(
            cloud_task_id="my-task-id",
            num_validations_run=5,
            validations_runtime_sec=mock.ANY,
            validation_run_id=mock.ANY,
            ingest_instance=DirectIngestInstance.PRIMARY,
            sandbox_dataset_prefix=None,
        )

        expected_labels = ["Validation", "Region: US_XX", "Team: State Pod"]
        expected_calls = [
            call(
                title="[staging][US_XX] `test_2`",
                body=mock.ANY,
                labels=expected_labels,
            ),
            call(
                title="[staging][US_XX] `test_4`",
                body=mock.ANY,
                labels=expected_labels,
            ),
        ]
        self.assertCountEqual(
            mock_github_repo.create_issue.call_args_list, expected_calls
        )

    @patch("recidiviz.validation.validation_manager._emit_opencensus_failure_events")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    @patch(
        "recidiviz.validation.validation_manager.store_validation_results_in_big_query"
    )
    @patch(
        "recidiviz.validation.validation_manager.store_validation_run_completion_in_big_query"
    )
    def test_handle_request_happy_path_nothing_configured(
        self,
        mock_store_run_success: MagicMock,
        mock_store_validation_results: MagicMock,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_opencensus_failure_events: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = []

        data = {"ingest_instance": "PRIMARY"}
        response = self.client.post(
            "/validate/US_XX", headers=APP_ENGINE_HEADERS, data=json.dumps(data)
        )

        self.assertEqual(200, response.status_code)

        mock_run_job.assert_not_called()
        mock_emit_opencensus_failure_events.assert_not_called()
        mock_store_validation_results.assert_called_once()
        ((results,), _kwargs) = mock_store_validation_results.call_args
        self.assertEqual(0, len(results))

        mock_store_run_success.assert_called_with(
            cloud_task_id="my-task-id",
            num_validations_run=0,
            validations_runtime_sec=mock.ANY,
            validation_run_id=mock.ANY,
            ingest_instance=DirectIngestInstance.PRIMARY,
            sandbox_dataset_prefix=None,
        )


class TestExecuteValidationRequest(TestCase):
    """Tests for execute_validation_request."""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"
        self.environment_patcher = patch(
            "recidiviz.validation.validation_manager.get_environment_for_project"
        )
        self.environment_patcher.start().return_value = GCPEnvironment.STAGING

        self._TEST_VALIDATIONS = get_test_validations()

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.environment_patcher.stop()

    @patch(
        "recidiviz.validation.validation_manager._file_tickets_for_failing_validations"
    )
    @patch("recidiviz.validation.validation_manager._emit_opencensus_failure_events")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    @patch(
        "recidiviz.validation.validation_manager.store_validation_results_in_big_query"
    )
    @patch(
        "recidiviz.validation.validation_manager.store_validation_run_completion_in_big_query"
    )
    def test_execute_validation_request_happy_path_no_failures(
        self,
        mock_store_run_success: MagicMock,
        mock_store_validation_results: MagicMock,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_opencensus_failure_events: MagicMock,
        mock_file_tickets_for_failing_validations: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS
        mock_run_job.return_value = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[0],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.SUCCESS
            ),
        )

        execute_validation_request(
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        self.assertEqual(5, mock_run_job.call_count)
        for job in self._TEST_VALIDATIONS:
            mock_run_job.assert_any_call(job)

        mock_emit_opencensus_failure_events.assert_not_called()
        mock_file_tickets_for_failing_validations.assert_not_called()
        mock_store_validation_results.assert_called_once()
        ((results,), _kwargs) = mock_store_validation_results.call_args
        self.assertEqual(5, len(results))

        mock_store_run_success.assert_called_with(
            cloud_task_id="AIRFLOW_VALIDATION",
            num_validations_run=5,
            validations_runtime_sec=mock.ANY,
            validation_run_id=mock.ANY,
            ingest_instance=DirectIngestInstance.PRIMARY,
            sandbox_dataset_prefix=None,
        )

    @patch(
        "recidiviz.validation.validation_manager._file_tickets_for_failing_validations"
    )
    @patch("recidiviz.validation.validation_manager._emit_opencensus_failure_events")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    @patch(
        "recidiviz.validation.validation_manager.store_validation_results_in_big_query"
    )
    @patch(
        "recidiviz.validation.validation_manager.store_validation_run_completion_in_big_query"
    )
    def test_execute_validation_request_happy_path_no_failures_secondary_with_sandbox_prefix(
        self,
        mock_store_run_success: MagicMock,
        mock_store_validation_results: MagicMock,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_opencensus_failure_events: MagicMock,
        mock_file_tickets_for_failing_validations: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS
        mock_run_job.return_value = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[0],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.SUCCESS
            ),
        )

        execute_validation_request(
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.SECONDARY,
            sandbox_prefix="test_prefix",
        )

        self.assertEqual(5, mock_run_job.call_count)
        for job in self._TEST_VALIDATIONS:
            mock_run_job.assert_any_call(job)

        mock_emit_opencensus_failure_events.assert_not_called()
        mock_file_tickets_for_failing_validations.assert_not_called()
        mock_store_validation_results.assert_called_once()
        ((results,), _kwargs) = mock_store_validation_results.call_args
        self.assertEqual(5, len(results))

        mock_store_run_success.assert_called_with(
            cloud_task_id="AIRFLOW_VALIDATION",
            num_validations_run=5,
            validations_runtime_sec=mock.ANY,
            validation_run_id=mock.ANY,
            ingest_instance=DirectIngestInstance.SECONDARY,
            sandbox_dataset_prefix="test_prefix",
        )

    def test_execute_validation_request_should_failure_secondary_with_no_sandbox_prefix(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Sandbox prefix must be specified for secondary ingest instance",
        ):
            execute_validation_request(
                state_code=StateCode.US_XX,
                ingest_instance=DirectIngestInstance.SECONDARY,
            )

    @patch("recidiviz.validation.validation_manager.github_helperbot_client")
    @patch("recidiviz.validation.validation_manager._emit_opencensus_failure_events")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    @patch(
        "recidiviz.validation.validation_manager.store_validation_results_in_big_query"
    )
    @patch(
        "recidiviz.validation.validation_manager.store_validation_run_completion_in_big_query"
    )
    def test_execute_validation_request_with_job_failures_and_validation_failures(
        self,
        mock_store_run_success: MagicMock,
        mock_store_validation_results: MagicMock,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_opencensus_failure_events: MagicMock,
        mock_github_client: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS
        mock_github_repo = MagicMock()
        mock_github_repo.get_issues.return_value = [
            Issue(
                requester=MagicMock(),
                headers=MagicMock(),
                attributes={"title": "[staging][US_XX] `test_3`"},
                completed=MagicMock(),
            )
        ]
        mock_github_client.return_value.get_repo.return_value = mock_github_repo
        first_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[1],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD
            ),
        )
        second_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[2],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD
            ),
        )
        third_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[3],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD, dev_mode=True
            ),
        )
        mock_run_job.side_effect = [
            DataValidationJobResult(
                validation_job=self._TEST_VALIDATIONS[0],
                result_details=FakeValidationResultDetails(
                    validation_status=ValidationResultStatus.SUCCESS
                ),
            ),
            first_failure,
            second_failure,
            third_failure,
            ValueError("Job failed to run!"),
        ]

        execute_validation_request(
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        self.assertEqual(len(self._TEST_VALIDATIONS), mock_run_job.call_count)
        for job in self._TEST_VALIDATIONS:
            mock_run_job.assert_any_call(job)

        mock_emit_opencensus_failure_events.assert_called_with(
            UnorderedCollection([self._TEST_VALIDATIONS[4]]),
            UnorderedCollection([first_failure, second_failure]),
        )
        mock_store_validation_results.assert_called_once()
        ((results,), _kwargs) = mock_store_validation_results.call_args
        self.assertEqual(5, len(results))

        mock_store_run_success.assert_called_with(
            cloud_task_id="AIRFLOW_VALIDATION",
            num_validations_run=5,
            validations_runtime_sec=mock.ANY,
            validation_run_id=mock.ANY,
            ingest_instance=DirectIngestInstance.PRIMARY,
            sandbox_dataset_prefix=None,
        )

        expected_labels = ["Validation", "Region: US_XX", "Team: State Pod"]
        expected_calls = [
            call(
                title="[staging][US_XX] `test_2`",
                body=mock.ANY,
                labels=expected_labels,
            ),
            call(
                title="[staging][US_XX] `test_4`",
                body=mock.ANY,
                labels=expected_labels,
            ),
        ]
        self.assertCountEqual(
            mock_github_repo.create_issue.call_args_list, expected_calls
        )

    @patch("recidiviz.validation.validation_manager.github_helperbot_client")
    @patch("recidiviz.validation.validation_manager._emit_opencensus_failure_events")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    @patch(
        "recidiviz.validation.validation_manager.store_validation_results_in_big_query"
    )
    @patch(
        "recidiviz.validation.validation_manager.store_validation_run_completion_in_big_query"
    )
    def test_execute_validation_request_happy_path_some_failures(
        self,
        mock_store_run_success: MagicMock,
        mock_store_validation_results: MagicMock,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_opencensus_failure_events: MagicMock,
        mock_github_client: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS
        mock_github_repo = MagicMock()
        mock_github_repo.get_issues.return_value = [
            Issue(
                requester=MagicMock(),
                headers=MagicMock(),
                attributes={"title": "[staging][US_XX] `test_3`"},
                completed=MagicMock(),
            )
        ]
        mock_github_client.return_value.get_repo.return_value = mock_github_repo

        first_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[1],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD
            ),
        )
        second_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[2],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD
            ),
        )
        third_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[3],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD, dev_mode=True
            ),
        )
        mock_run_job.side_effect = [
            DataValidationJobResult(
                validation_job=self._TEST_VALIDATIONS[0],
                result_details=FakeValidationResultDetails(
                    validation_status=ValidationResultStatus.SUCCESS
                ),
            ),
            first_failure,
            second_failure,
            third_failure,
            DataValidationJobResult(
                validation_job=self._TEST_VALIDATIONS[4],
                result_details=FakeValidationResultDetails(
                    validation_status=ValidationResultStatus.SUCCESS
                ),
            ),
        ]

        execute_validation_request(
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        self.assertEqual(5, mock_run_job.call_count)
        for job in self._TEST_VALIDATIONS:
            mock_run_job.assert_any_call(job)

        mock_emit_opencensus_failure_events.assert_called_with(
            [], UnorderedCollection([first_failure, second_failure])
        )
        mock_store_validation_results.assert_called_once()
        self.assertEqual(1, len(mock_store_validation_results.call_args[0]))
        ((results,), _kwargs) = mock_store_validation_results.call_args
        self.assertEqual(5, len(results))

        mock_store_run_success.assert_called_with(
            cloud_task_id="AIRFLOW_VALIDATION",
            num_validations_run=5,
            validations_runtime_sec=mock.ANY,
            validation_run_id=mock.ANY,
            ingest_instance=DirectIngestInstance.PRIMARY,
            sandbox_dataset_prefix=None,
        )

        expected_labels = ["Validation", "Region: US_XX", "Team: State Pod"]
        expected_calls = [
            call(
                title="[staging][US_XX] `test_2`",
                body=mock.ANY,
                labels=expected_labels,
            ),
            call(
                title="[staging][US_XX] `test_4`",
                body=mock.ANY,
                labels=expected_labels,
            ),
        ]
        self.assertCountEqual(
            mock_github_repo.create_issue.call_args_list, expected_calls
        )

    @patch("recidiviz.validation.validation_manager._emit_opencensus_failure_events")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    @patch(
        "recidiviz.validation.validation_manager.store_validation_results_in_big_query"
    )
    @patch(
        "recidiviz.validation.validation_manager.store_validation_run_completion_in_big_query"
    )
    def test_execute_validation_request_happy_path_nothing_configured(
        self,
        mock_store_run_success: MagicMock,
        mock_store_validation_results: MagicMock,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_opencensus_failure_events: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = []

        execute_validation_request(
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        mock_run_job.assert_not_called()
        mock_emit_opencensus_failure_events.assert_not_called()
        mock_store_validation_results.assert_called_once()
        ((results,), _kwargs) = mock_store_validation_results.call_args
        self.assertEqual(0, len(results))

        mock_store_run_success.assert_called_with(
            cloud_task_id="AIRFLOW_VALIDATION",
            num_validations_run=0,
            validations_runtime_sec=mock.ANY,
            validation_run_id=mock.ANY,
            ingest_instance=DirectIngestInstance.PRIMARY,
            sandbox_dataset_prefix=None,
        )

    @patch(
        "recidiviz.validation.validation_manager._file_tickets_for_failing_validations"
    )
    @patch("recidiviz.validation.validation_manager._emit_opencensus_failure_events")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    @patch(
        "recidiviz.validation.validation_manager.store_validation_results_in_big_query"
    )
    @patch(
        "recidiviz.validation.validation_manager.store_validation_run_completion_in_big_query"
    )
    def test_execute_validation_files_tickets(
        self,
        _mock_store_run_success: MagicMock,
        _mock_store_validation_results: MagicMock,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        _mock_emit_opencensus_failure_events: MagicMock,
        mock_file_tickets_for_failing_validations: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS
        first_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[1],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD
            ),
        )
        second_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[2],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD
            ),
        )
        third_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[3],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD, dev_mode=True
            ),
        )
        mock_run_job.side_effect = [
            DataValidationJobResult(
                validation_job=self._TEST_VALIDATIONS[0],
                result_details=FakeValidationResultDetails(
                    validation_status=ValidationResultStatus.SUCCESS
                ),
            ),
            first_failure,
            second_failure,
            third_failure,
            ValueError("Job failed to run!"),
        ]
        _ = execute_validation(
            region_code=StateCode.US_XX.value,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )
        mock_file_tickets_for_failing_validations.assert_called()

    @patch(
        "recidiviz.validation.validation_manager._file_tickets_for_failing_validations"
    )
    @patch("recidiviz.validation.validation_manager._emit_opencensus_failure_events")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    @patch(
        "recidiviz.validation.validation_manager.store_validation_results_in_big_query"
    )
    @patch(
        "recidiviz.validation.validation_manager.store_validation_run_completion_in_big_query"
    )
    def test_execute_validation_doesnt_file_tickets(
        self,
        _mock_store_run_success: MagicMock,
        _mock_store_validation_results: MagicMock,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        _mock_emit_opencensus_failure_events: MagicMock,
        mock_file_tickets_for_failing_validations: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS
        first_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[1],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD
            ),
        )
        second_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[2],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD
            ),
        )
        third_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[3],
            result_details=FakeValidationResultDetails(
                validation_status=ValidationResultStatus.FAIL_HARD, dev_mode=True
            ),
        )
        mock_run_job.side_effect = [
            DataValidationJobResult(
                validation_job=self._TEST_VALIDATIONS[0],
                result_details=FakeValidationResultDetails(
                    validation_status=ValidationResultStatus.SUCCESS
                ),
            ),
            first_failure,
            second_failure,
            third_failure,
            ValueError("Job failed to run!"),
        ]
        _ = execute_validation(
            region_code=StateCode.US_XX.value,
            ingest_instance=DirectIngestInstance.PRIMARY,
            file_tickets_on_failure=False,
        )
        mock_file_tickets_for_failing_validations.assert_not_called()


class TestFetchValidations(TestCase):
    """Tests the _fetch_validation_jobs_to_perform function."""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-456"

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    @patch(
        "recidiviz.utils.environment.get_gcp_environment",
        return_value=GCPEnvironment.STAGING.value,
    )
    def test_cross_product_states_and_checks_staging(
        self, _mock_get_environment: MagicMock
    ) -> None:
        all_validations = get_all_validations()
        all_region_configs = get_validation_region_configs()
        global_config = get_validation_global_config()

        for state_code, config in all_region_configs.items():
            num_exclusions = len(config.exclusions) + len(global_config.disabled)
            expected_length = len(all_validations) - num_exclusions
            result = _fetch_validation_jobs_to_perform(
                region_code=state_code, ingest_instance=DirectIngestInstance.PRIMARY
            )
            self.assertEqual(expected_length, len(result))

    @patch(
        "recidiviz.utils.environment.get_gcp_environment",
        return_value=GCPEnvironment.PRODUCTION.value,
    )
    def test_cross_product_states_and_checks_production(
        self, _mock_get_environment: MagicMock
    ) -> None:
        all_validations = get_all_validations()
        region_configs_to_validate = get_validation_region_configs()
        global_config = get_validation_global_config()
        launched_state_codes = [
            region
            for region, config in region_configs_to_validate.items()
            if not config.dev_mode
        ]

        # When you promote a state to production, we will start running validations against that state - add it to this
        # list to confirm you've updated all relevant external data validation tables in production to include
        # validation data for the newly promoted region.
        self.assertCountEqual(
            launched_state_codes,
            [
                "US_CO",
                "US_MO",
                "US_ND",
                "US_PA",
                "US_TN",
                "US_MI",
                "US_ME",
                "US_IX",
            ],
        )

        for state_code, config in region_configs_to_validate.items():
            num_exclusions = len(config.exclusions) + len(global_config.disabled)
            expected_length = len(all_validations) - num_exclusions
            result = _fetch_validation_jobs_to_perform(
                region_code=state_code, ingest_instance=DirectIngestInstance.PRIMARY
            )
            self.assertEqual(expected_length, len(result))

    @patch("recidiviz.validation.validation_manager.get_validation_region_configs")
    @patch("recidiviz.validation.validation_manager.get_all_validations")
    def test_fetch_validation_jobs_to_perform_applies_configs(
        self,
        mock_get_all_validations_fn: MagicMock,
        mock_get_region_configs_fn: MagicMock,
    ) -> None:
        existence_builder = SimpleBigQueryViewBuilder(
            project_id="my_project",
            dataset_id="my_dataset",
            view_id="existence_view",
            description="existence_view description",
            view_query_template="SELECT NULL LIMIT 0",
        )
        sameness_builder = SimpleBigQueryViewBuilder(
            project_id="my_project",
            dataset_id="my_dataset",
            view_id="sameness_view",
            description="sameness_view description",
            view_query_template="SELECT NULL LIMIT 1",
        )
        mock_get_region_configs_fn.return_value = {
            "US_XX": ValidationRegionConfig(
                region_code="US_XX",
                dev_mode=False,
                exclusions={},
                num_allowed_rows_overrides={
                    existence_builder.view_id: ValidationNumAllowedRowsOverride(
                        region_code="US_XX",
                        validation_name=existence_builder.view_id,
                        hard_num_allowed_rows_override=10,
                        soft_num_allowed_rows_override=10,
                        override_reason="This is broken",
                    )
                },
                max_allowed_error_overrides={
                    sameness_builder.view_id: ValidationMaxAllowedErrorOverride(
                        region_code="US_XX",
                        validation_name=sameness_builder.view_id,
                        hard_max_allowed_error_override=0.3,
                        soft_max_allowed_error_override=0.3,
                        override_reason="This is also broken",
                    )
                },
            ),
            "US_YY": ValidationRegionConfig(
                region_code="US_YY",
                dev_mode=True,
                exclusions={},
                num_allowed_rows_overrides={},
                max_allowed_error_overrides={},
            ),
        }
        region_configs = mock_get_region_configs_fn()

        mock_get_all_validations_fn.return_value = [
            ExistenceDataValidationCheck(
                view_builder=existence_builder,
                validation_category=ValidationCategory.INVARIANT,
            ),
            SamenessDataValidationCheck(
                view_builder=sameness_builder,
                comparison_columns=["col1", "col2"],
                validation_category=ValidationCategory.CONSISTENCY,
                region_configs=region_configs,
            ),
        ]

        actual_jobs = []
        for state_code in region_configs:
            actual_jobs.extend(
                _fetch_validation_jobs_to_perform(
                    state_code, DirectIngestInstance.PRIMARY
                )
            )

        expected_jobs = [
            DataValidationJob(
                validation=ExistenceDataValidationCheck(
                    validation_category=ValidationCategory.INVARIANT,
                    view_builder=existence_builder,
                    validation_name_suffix=None,
                    validation_type=ValidationCheckType.EXISTENCE,
                    dev_mode=False,
                    hard_num_allowed_rows=10,
                    soft_num_allowed_rows=10,
                ),
                region_code="US_XX",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ),
            DataValidationJob(
                validation=SamenessDataValidationCheck(
                    validation_category=ValidationCategory.CONSISTENCY,
                    view_builder=sameness_builder,
                    validation_name_suffix=None,
                    comparison_columns=["col1", "col2"],
                    sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
                    dev_mode=False,
                    hard_max_allowed_error=0.3,
                    soft_max_allowed_error=0.3,
                    validation_type=ValidationCheckType.SAMENESS,
                    region_configs=region_configs,
                ),
                region_code="US_XX",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ),
            DataValidationJob(
                validation=ExistenceDataValidationCheck(
                    validation_category=ValidationCategory.INVARIANT,
                    view_builder=existence_builder,
                    validation_name_suffix=None,
                    validation_type=ValidationCheckType.EXISTENCE,
                    dev_mode=True,
                    hard_num_allowed_rows=0,
                ),  # No override
                region_code="US_YY",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ),
            DataValidationJob(
                validation=SamenessDataValidationCheck(
                    validation_category=ValidationCategory.CONSISTENCY,
                    view_builder=sameness_builder,
                    validation_name_suffix=None,
                    comparison_columns=["col1", "col2"],
                    sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
                    dev_mode=True,
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                    validation_type=ValidationCheckType.SAMENESS,
                    region_configs=region_configs,
                ),
                region_code="US_YY",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ),
        ]
        self.assertEqual(expected_jobs, actual_jobs)

    def test_all_validations_no_overlapping_names(self) -> None:
        all_validations = get_all_validations()

        all_names: Set[str] = set()
        for validation in all_validations:
            self.assertNotIn(validation.validation_name, all_names)
            all_names.add(validation.validation_name)

    def test_configs_all_reference_real_validations(self) -> None:
        validation_names = {
            validation.validation_name for validation in get_all_validations()
        }
        region_configs_to_validate = get_validation_region_configs()
        global_config = get_validation_global_config()

        global_disabled_names = {
            validation.validation_name for validation in global_config.disabled.values()
        }

        global_names_not_in_validations_list = global_disabled_names.difference(
            validation_names
        )
        self.assertEqual(
            set(),
            global_names_not_in_validations_list,
            f"Found views referenced in global config that do not exist in validations list: "
            f"{global_names_not_in_validations_list}",
        )

        for region_code, region_config in region_configs_to_validate.items():
            region_validation_names = {
                exclusion.validation_name
                for exclusion in region_config.exclusions.values()
            }
            region_validation_names.update(
                {
                    override.validation_name
                    for override in region_config.max_allowed_error_overrides.values()
                }
            )
            region_validation_names.update(
                {
                    override.validation_name
                    for override in region_config.num_allowed_rows_overrides.values()
                }
            )
            region_names_not_in_validations_list = region_validation_names.difference(
                validation_names
            )
            self.assertEqual(
                set(),
                region_names_not_in_validations_list,
                f"Found views referenced in region [{region_code}] config that do not exist in validations"
                f" list: {global_names_not_in_validations_list}",
            )

    def test_all_builders_referenced_by_validations_are_in_view_config(self) -> None:
        builders_in_validations = {v.view_builder for v in get_all_validations()}
        validation_views_not_in_view_config = builders_in_validations.difference(
            validation_view_config.get_view_builders_for_views_to_update()
        )

        self.assertEqual(set(), validation_views_not_in_view_config)

    def test_validation_job_returns_correct_query(self) -> None:
        builder = SimpleBigQueryViewBuilder(
            dataset_id="my_dataset",
            view_id="test_2",
            description="test_2 description",
            view_query_template="select * from literally_anything",
        )

        address_overrides_builder = BigQueryAddressOverrides.Builder(
            sandbox_prefix="prefix"
        )
        address_overrides_builder.register_custom_dataset_override(
            "my_dataset", "my_dataset_override"
        )
        address_overrides = address_overrides_builder.build()

        existence_check_job = DataValidationJob(
            validation=ExistenceDataValidationCheck(
                view_builder=builder, validation_category=ValidationCategory.INVARIANT
            ),
            region_code="US_XX",
        )

        self.assertEqual(
            "SELECT * FROM `recidiviz-456.my_dataset.test_2` WHERE region_code = 'US_XX'",
            existence_check_job.original_builder_query_str(),
        )

        existence_check_job = DataValidationJob(
            validation=ExistenceDataValidationCheck(
                view_builder=builder, validation_category=ValidationCategory.INVARIANT
            ),
            region_code="US_XX",
            address_overrides=address_overrides,
        )

        self.assertEqual(
            "SELECT * FROM `recidiviz-456.my_dataset_override.test_2` WHERE region_code = 'US_XX'",
            existence_check_job.original_builder_query_str(),
        )
