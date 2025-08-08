# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for validation_result_for_storage"""
import datetime
import unittest
from unittest import mock
from unittest.mock import create_autospec

import pytz
from freezegun import freeze_time
from google.cloud import bigquery
from mock import MagicMock, patch

from recidiviz.big_query import big_query_client
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.validation.checks.sameness_check import (
    ResultRow,
    SamenessDataValidationCheck,
    SamenessDataValidationCheckType,
    SamenessPerRowValidationResultDetails,
    SamenessPerViewValidationResultDetails,
)
from recidiviz.validation.validation_models import (
    DataValidationJob,
    DataValidationJobResult,
    ValidationCategory,
    ValidationCheckType,
    ValidationResultStatus,
)
from recidiviz.validation.validation_outputs import (
    store_validation_results_in_big_query,
    store_validation_run_completion_in_big_query,
)
from recidiviz.validation.validation_result_for_storage import (
    ValidationResultForStorage,
)


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch(
    "recidiviz.utils.environment.get_data_platform_version",
    MagicMock(return_value="v1.0.0"),
)
class TestValidationResultStorage(unittest.TestCase):
    """Tests for validation_result_for_storage"""

    def setUp(self) -> None:
        # Reset client caching
        # pylint: disable=protected-access
        big_query_client._clients_by_project_id_by_region.clear()

        self.trace_patcher = mock.patch(
            "recidiviz.validation.validation_result_for_storage.get_current_trace_id"
        )
        self.trace_patcher.start().return_value = "trace-id"

    def tearDown(self) -> None:
        self.trace_patcher.stop()

    def test_from_successful_result_per_row(self) -> None:
        # Arrange
        job_result = DataValidationJobResult(
            validation_job=DataValidationJob(
                region_code="US_XX",
                validation=SamenessDataValidationCheck(
                    validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                    validation_type=ValidationCheckType.SAMENESS,
                    comparison_columns=["a", "b", "c"],
                    sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
                    view_builder=SimpleBigQueryViewBuilder(
                        dataset_id="my_dataset",
                        view_id="test_view",
                        description="test_view description",
                        view_query_template="select * from literally_anything",
                    ),
                    region_configs={},
                ),
                sandbox_context=BigQueryViewSandboxContext(
                    output_sandbox_dataset_prefix="test_prefix",
                    parent_address_overrides=None,
                    parent_address_formatter_provider=None,
                    state_code_filter=None,
                ),
            ),
            result_details=SamenessPerRowValidationResultDetails(
                failed_rows=[],
                hard_max_allowed_error=0.0,
                soft_max_allowed_error=0.0,
                total_num_rows=924,
            ),
        )

        # Act
        result = ValidationResultForStorage.from_validation_result(
            run_id="abc123",
            run_datetime=datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.UTC),
            result=job_result,
            runtime_seconds=1,
        )

        # Assert
        self.assertEqual(
            ValidationResultForStorage(
                run_id="abc123",
                run_date=datetime.date(2000, 1, 1),
                run_datetime=datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.UTC),
                system_version="v1.0.0",
                check_type=ValidationCheckType.SAMENESS,
                validation_name="test_view",
                region_code="US_XX",
                did_run=True,
                validation_result_status=ValidationResultStatus.SUCCESS,
                failure_description=None,
                result_details_type="SamenessPerRowValidationResultDetails",
                result_details=SamenessPerRowValidationResultDetails(
                    failed_rows=[],
                    hard_max_allowed_error=0.0,
                    soft_max_allowed_error=0.0,
                    total_num_rows=924,
                ),
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                exception_log=None,
                runtime_seconds=1,
                ingest_instance=DirectIngestInstance.PRIMARY,
                sandbox_dataset_prefix="test_prefix",
            ),
            result,
        )
        self.assertEqual(
            {
                "run_id": "abc123",
                "run_date": "2000-01-01",
                "run_datetime": "2000-01-01T00:00:00",
                "system_version": "v1.0.0",
                "check_type": "SAMENESS",
                "validation_name": "test_view",
                "region_code": "US_XX",
                "did_run": True,
                "validation_result_status": "SUCCESS",
                "failure_description": None,
                "result_details_type": "SamenessPerRowValidationResultDetails",
                "result_details": '{"failed_rows": [], "hard_max_allowed_error": 0.0, "soft_max_allowed_error": 0.0, "total_num_rows": 924, "dev_mode": false}',
                "validation_category": "EXTERNAL_AGGREGATE",
                "exception_log": None,
                "trace_id": result.trace_id,
                "runtime_seconds": 1,
                "ingest_instance": "PRIMARY",
                "sandbox_dataset_prefix": "test_prefix",
            },
            result.to_serializable(),
        )

    def test_from_successful_result_per_view(self) -> None:
        # Arrange
        result_details = SamenessPerViewValidationResultDetails(
            num_error_rows=0,
            total_num_rows=5,
            hard_max_allowed_error=0.5,
            soft_max_allowed_error=0.5,
            non_null_counts_per_column_per_partition=[
                (("US_XX", "2020-12-01"), {"internal": 5, "external": 5})
            ],
        )
        job_result = DataValidationJobResult(
            validation_job=DataValidationJob(
                region_code="US_XX",
                validation=SamenessDataValidationCheck(
                    validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                    validation_type=ValidationCheckType.SAMENESS,
                    comparison_columns=["internal", "external"],
                    partition_columns=["state_code", "date"],
                    sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                    view_builder=SimpleBigQueryViewBuilder(
                        dataset_id="my_dataset",
                        view_id="test_view",
                        description="test_view description",
                        view_query_template="select * from literally_anything",
                    ),
                ),
            ),
            result_details=result_details,
        )

        # Act
        result = ValidationResultForStorage.from_validation_result(
            run_id="abc123",
            run_datetime=datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.UTC),
            result=job_result,
            runtime_seconds=1,
        )

        # Assert
        self.assertEqual(
            ValidationResultForStorage(
                run_id="abc123",
                run_date=datetime.date(2000, 1, 1),
                run_datetime=datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.UTC),
                system_version="v1.0.0",
                check_type=ValidationCheckType.SAMENESS,
                validation_name="test_view",
                region_code="US_XX",
                did_run=True,
                validation_result_status=ValidationResultStatus.SUCCESS,
                failure_description=None,
                result_details_type="SamenessPerViewValidationResultDetails",
                result_details=result_details,
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                exception_log=None,
                runtime_seconds=1,
                ingest_instance=DirectIngestInstance.PRIMARY,
                sandbox_dataset_prefix=None,
            ),
            result,
        )
        self.assertEqual(
            {
                "run_id": "abc123",
                "run_date": "2000-01-01",
                "run_datetime": "2000-01-01T00:00:00",
                "system_version": "v1.0.0",
                "check_type": "SAMENESS",
                "validation_name": "test_view",
                "region_code": "US_XX",
                "did_run": True,
                "validation_result_status": "SUCCESS",
                "failure_description": None,
                "result_details_type": "SamenessPerViewValidationResultDetails",
                "result_details": '{"num_error_rows": 0, "total_num_rows": 5, "hard_max_allowed_error": 0.5, "soft_max_allowed_error": 0.5, "dev_mode": false, "non_null_counts_per_column_per_partition": [[["US_XX", "2020-12-01"], {"internal": 5, "external": 5}]]}',
                "validation_category": "EXTERNAL_INDIVIDUAL",
                "exception_log": None,
                "trace_id": result.trace_id,
                "runtime_seconds": 1,
                "ingest_instance": "PRIMARY",
                "sandbox_dataset_prefix": None,
            },
            result.to_serializable(),
        )

    def test_from_failed_result(self) -> None:
        # Arrange
        result_details = SamenessPerRowValidationResultDetails(
            failed_rows=[
                (ResultRow(label_values=("US_XX",), comparison_values=(5, 10)), 0.5)
            ],
            hard_max_allowed_error=0.0,
            soft_max_allowed_error=0.0,
            total_num_rows=8,
        )
        job_result = DataValidationJobResult(
            validation_job=DataValidationJob(
                region_code="US_XX",
                validation=SamenessDataValidationCheck(
                    validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                    validation_type=ValidationCheckType.SAMENESS,
                    comparison_columns=["a", "b", "c"],
                    sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
                    view_builder=SimpleBigQueryViewBuilder(
                        dataset_id="my_dataset",
                        view_id="test_view",
                        description="test_view description",
                        view_query_template="select * from literally_anything",
                    ),
                    region_configs={},
                ),
            ),
            result_details=result_details,
        )

        # Act
        result = ValidationResultForStorage.from_validation_result(
            run_id="abc123",
            run_datetime=datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.UTC),
            result=job_result,
            runtime_seconds=1,
        )

        # Assert
        self.assertEqual(
            ValidationResultForStorage(
                run_id="abc123",
                run_date=datetime.date(2000, 1, 1),
                run_datetime=datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.UTC),
                system_version="v1.0.0",
                check_type=ValidationCheckType.SAMENESS,
                validation_name="test_view",
                region_code="US_XX",
                did_run=True,
                validation_result_status=ValidationResultStatus.FAIL_HARD,
                failure_description="1 row(s) had unacceptable margins of error. "
                "Of those rows, 1 row(s) exceeded the hard threshold and 0 row(s) "
                "exceeded the soft threshold. The acceptable margin of error is only "
                "0.0 (hard) and 0.0 (soft), but the validation returned "
                "rows with errors as high as 0.5.",
                result_details_type="SamenessPerRowValidationResultDetails",
                result_details=result_details,
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                exception_log=None,
                ingest_instance=DirectIngestInstance.PRIMARY,
                sandbox_dataset_prefix=None,
                runtime_seconds=1,
            ),
            result,
        )
        self.assertEqual(
            {
                "run_id": "abc123",
                "run_date": "2000-01-01",
                "run_datetime": "2000-01-01T00:00:00",
                "system_version": "v1.0.0",
                "check_type": "SAMENESS",
                "validation_name": "test_view",
                "region_code": "US_XX",
                "did_run": True,
                "validation_result_status": "FAIL_HARD",
                "failure_description": "1 row(s) had unacceptable margins of error. Of those "
                "rows, 1 row(s) exceeded the hard threshold and 0 "
                "row(s) exceeded the soft threshold. The acceptable "
                "margin of error is only 0.0 (hard) and 0.0 (soft), "
                "but the validation returned rows with errors as high "
                "as 0.5.",
                "result_details": '{"failed_rows": [[{"label_values": ["US_XX"], '
                '"comparison_values": [5, 10]}, 0.5]], '
                '"hard_max_allowed_error": 0.0, "soft_max_allowed_error": '
                '0.0, "total_num_rows": 8, "dev_mode": false}',
                "result_details_type": "SamenessPerRowValidationResultDetails",
                "validation_category": "EXTERNAL_AGGREGATE",
                "trace_id": result.trace_id,
                "exception_log": None,
                "runtime_seconds": 1,
                "ingest_instance": "PRIMARY",
                "sandbox_dataset_prefix": None,
            },
            result.to_serializable(),
        )

    def test_from_failed_run(self) -> None:
        # Arrange
        validation_job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
                region_configs={},
            ),
        )

        # Act
        result = ValidationResultForStorage.from_validation_job(
            run_id="abc123",
            run_datetime=datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.UTC),
            job=validation_job,
            exception_log=None,
        )

        # Assert
        self.assertEqual(
            ValidationResultForStorage(
                run_id="abc123",
                run_date=datetime.date(2000, 1, 1),
                run_datetime=datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.UTC),
                system_version="v1.0.0",
                check_type=ValidationCheckType.SAMENESS,
                validation_name="test_view",
                region_code="US_XX",
                did_run=False,
                validation_result_status=None,
                failure_description=None,
                result_details_type=None,
                result_details=None,
                exception_log=None,
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                runtime_seconds=None,
                ingest_instance=DirectIngestInstance.PRIMARY,
                sandbox_dataset_prefix=None,
            ),
            result,
        )
        self.assertEqual(
            {
                "run_id": "abc123",
                "run_date": "2000-01-01",
                "run_datetime": "2000-01-01T00:00:00",
                "system_version": "v1.0.0",
                "check_type": "SAMENESS",
                "validation_name": "test_view",
                "region_code": "US_XX",
                "did_run": False,
                "validation_result_status": None,
                "failure_description": None,
                "result_details_type": None,
                "result_details": None,
                "exception_log": None,
                "trace_id": result.trace_id,
                "validation_category": "EXTERNAL_AGGREGATE",
                "runtime_seconds": None,
                "ingest_instance": "PRIMARY",
                "sandbox_dataset_prefix": None,
            },
            result.to_serializable(),
        )

    @patch("recidiviz.utils.environment.in_gcp", MagicMock(return_value=False))
    @patch("recidiviz.big_query.big_query_client.bigquery.Client")
    def test_store_results_not_in_gcp(
        self, mock_bigquery_client_class: MagicMock
    ) -> None:
        # Arrange
        mock_bigquery_client = mock_bigquery_client_class.return_value
        mock_bigquery_client.insert_rows.return_value = []
        mock_bigquery_client.get_table.return_value = "table_object"

        # Act
        store_validation_results_in_big_query(
            [
                ValidationResultForStorage(
                    run_id="abc123",
                    run_date=datetime.date(2000, 1, 1),
                    run_datetime=datetime.datetime(
                        2000, 1, 1, 0, 0, 0, tzinfo=pytz.UTC
                    ),
                    system_version="v1.0.0",
                    check_type=ValidationCheckType.SAMENESS,
                    validation_name="test_view",
                    region_code="US_XX",
                    did_run=True,
                    validation_result_status=ValidationResultStatus.SUCCESS,
                    failure_description=None,
                    result_details_type="SamenessPerRowValidationResultDetails",
                    result_details=SamenessPerRowValidationResultDetails(
                        failed_rows=[],
                        hard_max_allowed_error=0.0,
                        soft_max_allowed_error=0.0,
                    ),
                    validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                    exception_log=None,
                    runtime_seconds=1,
                    ingest_instance=None,
                    sandbox_dataset_prefix=None,
                ),
            ]
        )

        # Assert
        mock_bigquery_client.insert_rows.assert_not_called()

    @patch("recidiviz.utils.environment.in_gcp", MagicMock(return_value=True))
    @patch("recidiviz.big_query.big_query_client.bigquery.Client")
    def test_store_results_in_gcp(self, mock_bigquery_client_class: MagicMock) -> None:
        # Arrange
        mock_bigquery_client = mock_bigquery_client_class.return_value
        mock_bigquery_client.insert_rows.return_value = []
        mock_bigquery_client.get_table.return_value = "table_object"

        storage_result_1 = ValidationResultForStorage(
            run_id="abc123",
            run_date=datetime.date(2000, 1, 1),
            run_datetime=datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.UTC),
            system_version="v1.0.0",
            check_type=ValidationCheckType.SAMENESS,
            validation_name="test_view",
            region_code="US_XX",
            did_run=True,
            validation_result_status=ValidationResultStatus.SUCCESS,
            failure_description=None,
            result_details_type="SamenessPerRowValidationResultDetails",
            result_details=SamenessPerRowValidationResultDetails(
                failed_rows=[],
                hard_max_allowed_error=0.0,
                soft_max_allowed_error=0.0,
                total_num_rows=3,
            ),
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            exception_log=None,
            runtime_seconds=1,
            ingest_instance=None,
            sandbox_dataset_prefix=None,
        )
        storage_result_2 = ValidationResultForStorage(
            run_id="abc123",
            run_date=datetime.date(2000, 1, 1),
            run_datetime=datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.UTC),
            system_version="v1.0.0",
            check_type=ValidationCheckType.SAMENESS,
            validation_name="test_view",
            region_code="US_XX",
            did_run=True,
            validation_result_status=ValidationResultStatus.FAIL_HARD,
            failure_description="1 row(s) had unacceptable margins of error. The "
            "acceptable margin of error is only 0.0, but the validation returned "
            "rows with errors as high as 0.5.",
            result_details_type="SamenessPerRowValidationResultDetails",
            result_details=SamenessPerRowValidationResultDetails(
                failed_rows=[
                    (
                        ResultRow(label_values=("US_XX",), comparison_values=(5, 10)),
                        0.5,
                    )
                ],
                hard_max_allowed_error=0.0,
                soft_max_allowed_error=0.0,
                total_num_rows=5,
            ),
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            exception_log=None,
            runtime_seconds=1,
            ingest_instance=None,
            sandbox_dataset_prefix=None,
        )
        storage_result_3 = ValidationResultForStorage(
            run_id="abc123",
            run_date=datetime.date(2000, 1, 1),
            run_datetime=datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.UTC),
            system_version="v1.0.0",
            check_type=ValidationCheckType.SAMENESS,
            validation_name="test_view",
            region_code="US_XX",
            did_run=False,
            validation_result_status=None,
            failure_description=None,
            result_details_type=None,
            result_details=None,
            validation_category=ValidationCategory.CONSISTENCY,
            exception_log=None,
            runtime_seconds=1,
            ingest_instance=None,
            sandbox_dataset_prefix=None,
        )

        # Act
        store_validation_results_in_big_query(
            [
                storage_result_1,
                storage_result_2,
                storage_result_3,
            ]
        )

        # Assert
        mock_bigquery_client.insert_rows.assert_called_once_with(
            "table_object",
            [
                {
                    "run_id": "abc123",
                    "run_date": "2000-01-01",
                    "run_datetime": "2000-01-01T00:00:00",
                    "system_version": "v1.0.0",
                    "check_type": "SAMENESS",
                    "validation_name": "test_view",
                    "region_code": "US_XX",
                    "did_run": True,
                    "validation_result_status": "SUCCESS",
                    "failure_description": None,
                    "result_details_type": "SamenessPerRowValidationResultDetails",
                    "result_details": '{"failed_rows": [], "hard_max_allowed_error": 0.0, "soft_max_allowed_error": 0.0, "total_num_rows": 3, "dev_mode": false}',
                    "validation_category": "EXTERNAL_AGGREGATE",
                    "exception_log": None,
                    "trace_id": storage_result_1.trace_id,
                    "runtime_seconds": 1,
                    "ingest_instance": None,
                    "sandbox_dataset_prefix": None,
                },
                {
                    "run_id": "abc123",
                    "run_date": "2000-01-01",
                    "run_datetime": "2000-01-01T00:00:00",
                    "system_version": "v1.0.0",
                    "check_type": "SAMENESS",
                    "validation_name": "test_view",
                    "region_code": "US_XX",
                    "did_run": True,
                    "validation_result_status": "FAIL_HARD",
                    "failure_description": "1 row(s) had unacceptable margins of error. The acceptable margin of error is only 0.0, but the validation returned rows with errors as high as 0.5.",
                    "result_details_type": "SamenessPerRowValidationResultDetails",
                    "result_details": '{"failed_rows": [[{"label_values": ["US_XX"], "comparison_values": [5, 10]}, 0.5]], "hard_max_allowed_error": 0.0, "soft_max_allowed_error": 0.0, "total_num_rows": 5, "dev_mode": false}',
                    "validation_category": "EXTERNAL_AGGREGATE",
                    "exception_log": None,
                    "trace_id": storage_result_2.trace_id,
                    "runtime_seconds": 1,
                    "ingest_instance": None,
                    "sandbox_dataset_prefix": None,
                },
                {
                    "run_id": "abc123",
                    "run_date": "2000-01-01",
                    "run_datetime": "2000-01-01T00:00:00",
                    "system_version": "v1.0.0",
                    "check_type": "SAMENESS",
                    "validation_name": "test_view",
                    "region_code": "US_XX",
                    "did_run": False,
                    "validation_result_status": None,
                    "failure_description": None,
                    "result_details_type": None,
                    "result_details": None,
                    "validation_category": "CONSISTENCY",
                    "exception_log": None,
                    "trace_id": storage_result_3.trace_id,
                    "runtime_seconds": 1,
                    "ingest_instance": None,
                    "sandbox_dataset_prefix": None,
                },
            ],
        )

    @freeze_time("2022-01-01")
    @patch("recidiviz.utils.environment.in_gcp", MagicMock(return_value=True))
    @patch("recidiviz.big_query.big_query_client.bigquery.Client")
    def test_store_completion(self, mock_bigquery_client_class: MagicMock) -> None:
        # Arrange
        mock_bigquery_client = mock_bigquery_client_class.return_value
        mock_bigquery_client.insert_rows.return_value = []
        mock_table = create_autospec(bigquery.Table)
        mock_bigquery_client.get_table.return_value = mock_table

        # Act
        store_validation_run_completion_in_big_query(
            state_code=StateCode.US_XX,
            validation_run_id="abc123",
            num_validations_run=10,
            validations_runtime_sec=5,
            sandbox_dataset_prefix=None,
        )

        # Assert
        mock_bigquery_client.insert_rows.assert_called_once_with(
            mock_table,
            [
                {
                    "region_code": "US_XX",
                    "run_id": "abc123",
                    "success_timestamp": "2022-01-01T00:00:00+00:00",
                    "num_validations_run": 10,
                    "validations_runtime_sec": 5,
                    "ingest_instance": "PRIMARY",
                    "sandbox_dataset_prefix": None,
                }
            ],
        )

    @patch("recidiviz.utils.environment.in_gcp", MagicMock(return_value=False))
    @patch("recidiviz.big_query.big_query_client.bigquery.Client")
    def test_store_completion_not_in_gcp(
        self, mock_bigquery_client_class: MagicMock
    ) -> None:
        # Arrange
        mock_bigquery_client = mock_bigquery_client_class.return_value

        # Act
        store_validation_run_completion_in_big_query(
            state_code=StateCode.US_XX,
            validation_run_id="abc123",
            num_validations_run=10,
            validations_runtime_sec=5,
            sandbox_dataset_prefix=None,
        )

        # Assert
        mock_bigquery_client.insert_rows.assert_not_called()
