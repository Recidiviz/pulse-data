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
"""Tests for ValidationStatusStore"""
# TODO(protocolbuffers/protobuf#10372): Remove pylint no-name-in-module check
# pylint: disable=no-name-in-module
import datetime
import unittest

from fakeredis import FakeRedis
from google.protobuf.timestamp_pb2 import Timestamp  # pylint: disable=no-name-in-module
from mock import MagicMock, patch
from werkzeug.exceptions import ServiceUnavailable

from recidiviz.admin_panel.models.validation_pb2 import (
    SamenessPerViewValidationResultDetails,
    ValidationStatusRecord,
    ValidationStatusRecords,
)
from recidiviz.admin_panel.validation_metadata_store import ValidationStatusStore
from recidiviz.utils.environment import GCP_PROJECTS

_TEST_PROJECT = "test-project"


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value=_TEST_PROJECT))
class ValidationStatusStoreTest(unittest.TestCase):
    """Tests for ValidationStatusStore"""

    def setUp(self) -> None:
        self.redis_patcher = patch(
            "recidiviz.admin_panel.admin_panel_store.get_admin_panel_redis"
        )
        self.mock_redis_patcher = self.redis_patcher.start()
        self.mock_redis_patcher.return_value = FakeRedis()

    def tearDown(self) -> None:
        self.mock_redis_patcher.stop()

    def _check_queries_use_correct_project_id(
        self, mock_bigquery_client: MagicMock
    ) -> None:
        if not mock_bigquery_client.run_query_async.mock_calls:
            raise ValueError("Did not find any calls to run_query_async")

        for call in mock_bigquery_client.run_query_async.mock_calls:
            kwargs = call[2]
            query_str = kwargs["query_str"]
            for project_id in GCP_PROJECTS:
                self.assertFalse(
                    project_id in query_str,
                    f"Found substring '{project_id}' in the query string: {query_str} ",
                )
            self.assertTrue(
                _TEST_PROJECT in query_str,
                f"Did not find expected substring '{_TEST_PROJECT}' in the query string: {query_str} ",
            )

    @patch(
        "recidiviz.admin_panel.validation_metadata_store.BigQueryClientImpl",
        MagicMock(),
    )
    def test_not_ready(self) -> None:
        store = ValidationStatusStore()

        with self.assertRaises(ServiceUnavailable):
            store.get_most_recent_validation_results()

    @patch("recidiviz.admin_panel.validation_metadata_store.BigQueryClientImpl")
    def test_most_recent_validation_results(
        self, mock_bigquery_client_class: MagicMock
    ) -> None:
        mock_bigquery_client = mock_bigquery_client_class.return_value
        mock_bigquery_client.run_query_async.return_value = [
            {
                "run_id": "abc123",
                "run_datetime": datetime.datetime(2000, 1, 1, 0, 0, 0),
                "system_version": "v1.0.0",
                "validation_category": "EXTERNAL_INDIVIDUAL",
                "validation_name": "test_view",
                "region_code": "US_XX",
                "did_run": True,
                "validation_result_status": "SUCCESS",
                "result_details_type": "SamenessPerViewValidationResultDetails",
                "result_details": '{"num_error_rows": 12, "total_num_rows":1000, "hard_max_allowed_error": 0.02, "soft_max_allowed_error": 0.02, "non_null_counts_per_column_per_partition": []}',
            },
            {
                "run_id": "abc123",
                "run_datetime": datetime.datetime(2000, 1, 1, 0, 0, 0),
                "system_version": "v1.0.0",
                "validation_category": "EXTERNAL_INDIVIDUAL",
                "validation_name": "test_view",
                "region_code": "US_YY",
                "did_run": True,
                "validation_result_status": "FAIL_HARD",
                "result_details_type": "SamenessPerViewValidationResultDetails",
                "result_details": '{"num_error_rows": 999, "total_num_rows":1000, "hard_max_allowed_error": 0.02, "soft_max_allowed_error": 0.02, "non_null_counts_per_column_per_partition": []}',
                "failure_description": "",
            },
            {
                "run_id": "abc123",
                "run_datetime": datetime.datetime(2000, 1, 1, 0, 0, 0),
                "system_version": "v1.0.0",
                "validation_category": "CONSISTENCY",
                "validation_name": "other_view",
                "region_code": "US_XX",
                "did_run": False,
                "validation_result_status": None,
                "result_details_type": None,
                "result_details": None,
            },
        ]

        store = ValidationStatusStore()
        store.hydrate_cache()
        results = store.get_most_recent_validation_results()
        self._check_queries_use_correct_project_id(mock_bigquery_client)

        timestamp = Timestamp()
        timestamp.FromDatetime(datetime.datetime(2000, 1, 1, 0, 0, 0))

        self.assertEqual(
            ValidationStatusRecords(
                records=[
                    ValidationStatusRecord(
                        run_id="abc123",
                        run_datetime=timestamp,
                        system_version="v1.0.0",
                        name="test_view",
                        category=ValidationStatusRecord.ValidationCategory.EXTERNAL_INDIVIDUAL,
                        is_percentage=True,
                        state_code="US_XX",
                        did_run=True,
                        has_data=True,
                        dev_mode=False,
                        hard_failure_amount=0.02,
                        soft_failure_amount=0.02,
                        result_status=ValidationStatusRecord.ValidationResultStatus.SUCCESS,
                        error_amount=0.012,
                        failure_description=None,
                        existence=None,
                        sameness_per_row=None,
                        sameness_per_view=SamenessPerViewValidationResultDetails(
                            num_error_rows=12,
                            total_num_rows=1000,
                            non_null_counts_per_column_per_partition=[],
                        ),
                    ),
                    ValidationStatusRecord(
                        run_id="abc123",
                        run_datetime=timestamp,
                        system_version="v1.0.0",
                        name="test_view",
                        category=ValidationStatusRecord.ValidationCategory.EXTERNAL_INDIVIDUAL,
                        is_percentage=True,
                        state_code="US_YY",
                        did_run=True,
                        has_data=True,
                        dev_mode=False,
                        hard_failure_amount=0.02,
                        soft_failure_amount=0.02,
                        result_status=ValidationStatusRecord.ValidationResultStatus.FAIL_HARD,
                        error_amount=0.999,
                        failure_description="",
                        existence=None,
                        sameness_per_row=None,
                        sameness_per_view=SamenessPerViewValidationResultDetails(
                            num_error_rows=999,
                            total_num_rows=1000,
                            non_null_counts_per_column_per_partition=[],
                        ),
                    ),
                    ValidationStatusRecord(
                        run_id="abc123",
                        run_datetime=timestamp,
                        system_version="v1.0.0",
                        name="other_view",
                        category=ValidationStatusRecord.ValidationCategory.CONSISTENCY,
                        is_percentage=None,
                        state_code="US_XX",
                        did_run=False,
                        has_data=None,
                        dev_mode=None,
                        hard_failure_amount=None,
                        soft_failure_amount=None,
                        result_status=None,
                        error_amount=None,
                        failure_description=None,
                        existence=None,
                        sameness_per_row=None,
                        sameness_per_view=None,
                    ),
                ]
            ),
            results,
        )

    @patch("recidiviz.admin_panel.validation_metadata_store.BigQueryClientImpl")
    def test_multiple_states_different_run_ids_succeeds(
        self, mock_bigquery_client_class: MagicMock
    ) -> None:
        mock_bigquery_client = mock_bigquery_client_class.return_value
        mock_bigquery_client.run_query_async.return_value = [
            {
                "run_id": "abc123",
                "run_datetime": datetime.datetime(2000, 1, 1, 0, 0, 0),
                "system_version": "v1.0.0",
                "validation_category": "EXTERNAL_INDIVIDUAL",
                "validation_name": "test_view",
                "region_code": "US_XX",
                "did_run": True,
                "validation_result_status": "SUCCESS",
                "result_details_type": "SamenessPerViewValidationResultDetails",
                "result_details": '{"num_error_rows": 12, "total_num_rows":1000, "hard_max_allowed_error": 0.02, "soft_max_allowed_error": 0.02, "non_null_counts_per_column_per_partition": []}',
            },
            {
                "run_id": "def456",
                "run_datetime": datetime.datetime(2000, 1, 1, 0, 0, 0),
                "system_version": "v1.0.0",
                "validation_category": "EXTERNAL_INDIVIDUAL",
                "validation_name": "test_view",
                "region_code": "US_YY",
                "did_run": True,
                "validation_result_status": "FAIL_HARD",
                "result_details_type": "SamenessPerViewValidationResultDetails",
                "result_details": '{"num_error_rows": 999, "total_num_rows":1000, "hard_max_allowed_error": 0.02, "soft_max_allowed_error": 0.02, "non_null_counts_per_column_per_partition": []}',
                "failure_description": "",
            },
        ]

        store = ValidationStatusStore()
        store.hydrate_cache()
        results = store.get_most_recent_validation_results()
        self._check_queries_use_correct_project_id(mock_bigquery_client)

        timestamp = Timestamp()
        timestamp.FromDatetime(datetime.datetime(2000, 1, 1, 0, 0, 0))

        self.assertEqual(
            ValidationStatusRecords(
                records=[
                    ValidationStatusRecord(
                        run_id="abc123",
                        run_datetime=timestamp,
                        system_version="v1.0.0",
                        name="test_view",
                        category=ValidationStatusRecord.ValidationCategory.EXTERNAL_INDIVIDUAL,
                        is_percentage=True,
                        state_code="US_XX",
                        did_run=True,
                        has_data=True,
                        dev_mode=False,
                        hard_failure_amount=0.02,
                        soft_failure_amount=0.02,
                        result_status=ValidationStatusRecord.ValidationResultStatus.SUCCESS,
                        error_amount=0.012,
                        failure_description=None,
                        existence=None,
                        sameness_per_row=None,
                        sameness_per_view=SamenessPerViewValidationResultDetails(
                            num_error_rows=12,
                            total_num_rows=1000,
                            non_null_counts_per_column_per_partition=[],
                        ),
                    ),
                    ValidationStatusRecord(
                        run_id="def456",
                        run_datetime=timestamp,
                        system_version="v1.0.0",
                        name="test_view",
                        category=ValidationStatusRecord.ValidationCategory.EXTERNAL_INDIVIDUAL,
                        is_percentage=True,
                        state_code="US_YY",
                        did_run=True,
                        has_data=True,
                        dev_mode=False,
                        hard_failure_amount=0.02,
                        soft_failure_amount=0.02,
                        result_status=ValidationStatusRecord.ValidationResultStatus.FAIL_HARD,
                        error_amount=0.999,
                        failure_description="",
                        existence=None,
                        sameness_per_row=None,
                        sameness_per_view=SamenessPerViewValidationResultDetails(
                            num_error_rows=999,
                            total_num_rows=1000,
                            non_null_counts_per_column_per_partition=[],
                        ),
                    ),
                ]
            ),
            results,
        )
