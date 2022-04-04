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

import datetime
import unittest

from google.protobuf.timestamp_pb2 import Timestamp  # pylint: disable=no-name-in-module
from mock import MagicMock, patch
from werkzeug.exceptions import ServiceUnavailable

from recidiviz.admin_panel.models.validation_pb2 import (
    SamenessPerViewValidationResultDetails,
    ValidationStatusRecord,
    ValidationStatusRecords,
)
from recidiviz.admin_panel.validation_metadata_store import ValidationStatusStore


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
class ValidationStatusStoreTest(unittest.TestCase):
    """Tests for ValidationStatusStore"""

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
        store.recalculate_store()
        results = store.get_most_recent_validation_results()

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
    def test_multiple_run_ids_fails(
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
                "result_details": '{"num_error_rows": 99, "total_num_rows":1000, "hard_max_allowed_error": 0.02, "soft_max_allowed_error": 0.02, "non_null_counts_per_column_per_partition": []}',
                "failure_description": "",
            },
        ]

        store = ValidationStatusStore()
        with self.assertRaisesRegex(ValueError, "Expected single run id"):
            store.recalculate_store()
