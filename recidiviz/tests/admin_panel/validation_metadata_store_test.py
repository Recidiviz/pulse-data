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

from mock import MagicMock, patch
from werkzeug.exceptions import ServiceUnavailable

from recidiviz.admin_panel.validation_metadata_store import (
    ValidationStatusRecord,
    ValidationStatusResult,
    ValidationStatusResults,
    ValidationStatusStore,
)
from recidiviz.validation.validation_models import ValidationResultStatus


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
                "validation_result_status": ValidationResultStatus.SUCCESS,
                "result_details_type": "SamenessStringsValidationResultDetails",
                "has_data": True,
                "error_amount": 0.123456,
            },
            {
                "run_id": "abc123",
                "run_datetime": datetime.datetime(2000, 1, 1, 0, 0, 0),
                "system_version": "v1.0.0",
                "validation_category": "EXTERNAL_INDIVIDUAL",
                "validation_name": "test_view",
                "region_code": "US_YY",
                "did_run": True,
                "validation_result_status": ValidationResultStatus.FAIL_HARD,
                "result_details_type": "SamenessStringsValidationResultDetails",
                "has_data": True,
                "error_amount": 0.999,
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
                "has_data": None,
                "error_amount": None,
            },
        ]

        store = ValidationStatusStore()
        store.recalculate_store()
        results = store.get_most_recent_validation_results()

        self.assertEqual(
            results,
            ValidationStatusResults(
                runId="abc123",
                runDatetime=datetime.datetime(2000, 1, 1, 0, 0, 0),
                systemVersion="v1.0.0",
                results={
                    "test_view": ValidationStatusResult(
                        validationCategory="EXTERNAL_INDIVIDUAL",
                        resultsByState={
                            "US_XX": ValidationStatusRecord(
                                didRun=True,
                                wasSuccessful=True,
                                hasData=True,
                                errorAmount="12.3%",
                            ),
                            "US_YY": ValidationStatusRecord(
                                didRun=True,
                                wasSuccessful=False,
                                hasData=True,
                                errorAmount="99.9%",
                            ),
                        },
                    ),
                    "other_view": ValidationStatusResult(
                        validationCategory="CONSISTENCY",
                        resultsByState={
                            "US_XX": ValidationStatusRecord(
                                didRun=False,
                                wasSuccessful=None,
                                hasData=None,
                                errorAmount=None,
                            )
                        },
                    ),
                },
            ),
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
                "result_details_type": "SamenessNumbersValidationResultDetails",
                "has_data": True,
                "error_amount": 0.123456,
            },
            {
                "run_id": "def456",
                "run_datetime": datetime.datetime(2000, 1, 1, 0, 0, 0),
                "system_version": "v1.0.0",
                "validation_category": "EXTERNAL_INDIVIDUAL",
                "validation_name": "test_view",
                "region_code": "US_YY",
                "did_run": True,
                "validation_result_status": False,
                "result_details_type": "SamenessNumbersValidationResultDetails",
                "has_data": True,
                "error_amount": 0.999999,
            },
        ]

        store = ValidationStatusStore()
        with self.assertRaisesRegex(ValueError, "Expected single value"):
            store.recalculate_store()
