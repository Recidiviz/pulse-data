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
"""Tests for Intercom outbound content data export CSV header verification"""

import argparse
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.entrypoints.intercom_outbound_data_export import (
    IntercomOutboundDataExport,
    verify_headers,
)
from recidiviz.intercom.intercom_export_bq_table_manager import (
    IntercomExportBigQueryTableManager,
)
from recidiviz.intercom.manager import IntercomAPIManager


class TestIntercomCSVHeaderVerification(unittest.TestCase):
    """Tests for Intercom outbound content data export CSV header verification"""

    def setUp(self) -> None:
        self.output_dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.output_dir)

    def _write_csv(self, filename: str, header_line: str) -> str:
        path = os.path.join(self.output_dir, filename)
        with open(path, mode="w", encoding="utf-8") as f:
            f.write(header_line + "\n")
        return path

    def test_verify_headers(self) -> None:
        """
        Test that verify_headers() properly verifies correct headers.
        """
        file_paths = {
            "hard_bounce": self._write_csv(
                "hard_bounce_20260114.csv",
                "receipt_id,hard_bounced_at,update_datetime",
            ),
            "answer": self._write_csv(
                "answer_20260114.csv",
                "receipt_id,answered_at,question_id,question,response_type,response,update_datetime",
            ),
        }

        verify_headers(file_paths)

    def test_verify_headers_key_error(self) -> None:
        """
        Test that verify_headers() properly raises KeyError for CSV files that
        don't match the naming of any of the Intercom YAML files.
        """
        file_paths = {
            "call_back": self._write_csv(
                "call_back_20260114.csv",
                "id,event,update_datetime",
            ),
            "subscribe": self._write_csv(
                "subscribe_20260114.csv",
                "id,reason,update_datetime",
            ),
        }

        with self.assertRaises(KeyError) as context:
            verify_headers(file_paths)

        self.assertIn("No source table config found", str(context.exception))

    def test_verify_headers_column_not_found(self) -> None:
        """
        Test that verify_headers() properly raises ValueError for CSV files with
        headers that don't match the corresponding schema for the BigQuery table.
        """
        file_paths = {
            "receipt": self._write_csv(
                "receipt_20260114.csv",
                "id,event,update_datetime",
            ),
            "hard_bounce": self._write_csv(
                "hard_bounce_20260114.csv",
                "id,reason,update_datetime",
            ),
        }

        with self.assertRaises(ValueError) as context:
            verify_headers(file_paths)

        self.assertIn("not found in source table YAML", str(context.exception))

    def test_verify_headers_columns_incorrectly_ordered(self) -> None:
        """
        Test that verify_headers() properly raises ValueError for CSV files with
        headers that don't match the corresponding schema for the BigQuery table.
        """
        file_paths = {
            "hard_bounce": self._write_csv(
                "hard_bounce_20260114.csv",
                "hard_bounced_at,receipt_id,update_datetime",
            ),
            "answer": self._write_csv(
                "answer_20260114.csv",
                "answered_at,receipt_id,question_id,question,response_type,response,update_datetime",
            ),
        }

        with self.assertRaises(ValueError) as context:
            verify_headers(file_paths)

        self.assertIn(
            "CSV header does not match the ordering of the YAML schema fields",
            str(context.exception),
        )


class TestIntercomOutbountDataExport(unittest.TestCase):
    """Tests for IntercomOutbountDataExport"""

    def setUp(self) -> None:
        self.intercom_outbound_data_export = MagicMock(spec=IntercomOutboundDataExport)

        self.export_and_upload_intercom_data_patcher = patch(
            "recidiviz.entrypoints.intercom_outbound_data_export.export_and_upload_intercom_data",
            return_value=None,
        )
        self.mock_export_and_upload_intercom_data = (
            self.export_and_upload_intercom_data_patcher.start()
        )

        self.bq_table_manager_patcher = patch(
            "recidiviz.entrypoints.intercom_outbound_data_export.IntercomExportBigQueryTableManager",
            spec=IntercomExportBigQueryTableManager,
        )
        self.mock_bq_table_manager = self.bq_table_manager_patcher.start()

        # BigQueryClientImpl and IntercomAPIManager are patched to prevent real API calls
        self.bq_client_patcher = patch(
            "recidiviz.entrypoints.intercom_outbound_data_export.BigQueryClientImpl",
            spec=BigQueryClientImpl,
        )
        self.bq_client_patcher.start()

        self.intercom_api_manager_patcher = patch(
            "recidiviz.entrypoints.intercom_outbound_data_export.IntercomAPIManager",
            spec=IntercomAPIManager,
        )
        self.intercom_api_manager_patcher.start()

    def tearDown(self) -> None:
        self.export_and_upload_intercom_data_patcher.stop()
        self.bq_table_manager_patcher.stop()
        self.bq_client_patcher.stop()
        self.intercom_api_manager_patcher.stop()

    def helper_create_parsed_args(self, one_arg: bool = False) -> argparse.Namespace:
        """Returns argparse.Namespace object for IntercomOutboundDataExport"""

        if one_arg:
            args = [
                "--start-datetime-inclusive",
                "2026-07-21:11:07:00Z",
            ]
        else:
            args = [
                "--start-datetime-inclusive",
                "2026-07-21:11:07:00Z",
                "--end-datetime-inclusive",
                "2026-07-22:11:07:00Z",
            ]

        return IntercomOutboundDataExport.get_parser().parse_args(args)

    def test_run_entrypoint(self) -> None:
        """Tests that run_entrypoint() gets to the last function call without rasing errors"""

        IntercomOutboundDataExport.run_entrypoint(args=self.helper_create_parsed_args())
        self.mock_bq_table_manager.return_value.write_to_table.assert_called_once()

    def test_run_entrypoint_one_arg_provided(self) -> None:
        """Tests that run_entrypoint() properly raises ValueError when only one datetime arg is provided"""

        with self.assertRaises(ValueError) as context:
            IntercomOutboundDataExport.run_entrypoint(
                args=self.helper_create_parsed_args(one_arg=True)
            )

        self.assertIn(
            "start_datetime_inclusive and end_datetime_inclusive must either both be",
            str(context.exception),
        )

    def test_run_entrypoint_failure(self) -> None:
        """Tests that run_entrypoint() properly raises ValueError for IntercomCloudRunJobStatus.FAILURE"""

        # If any of the methods in export_intercom_data() raise an error, then the cloud
        # run job fails and run_entrypoint() raises a ValueError
        self.mock_export_and_upload_intercom_data.side_effect = KeyError()

        with self.assertRaises(ValueError) as context:
            IntercomOutboundDataExport.run_entrypoint(
                args=self.helper_create_parsed_args()
            )

        self.assertIn(
            "Intercom export cloud run job failed due to a failure in the Intercom data export.",
            str(context.exception),
        )
