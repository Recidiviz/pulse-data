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
"""Tests for IntercomExportBigQueryTableManager"""

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from google.cloud import exceptions
from google.cloud.bigquery import LoadJob, QueryJob

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.intercom.intercom_export_bq_table_manager import (
    IntercomExportBigQueryTableManager,
)
from recidiviz.intercom.intercom_export_columns import (
    EXPORT_DATETIME_COLUMN_NAME,
    EXPORT_WINDOW_END_INCLUSIVE_COLUMN_NAME,
    EXPORT_WINDOW_START_INCLUSIVE_COLUMN_NAME,
    STATUS_COLUMN_NAME,
    build_intercom_export_metadata_export_tracker_schema,
)
from recidiviz.intercom.types import IntercomCloudRunJobInfo, IntercomCloudRunJobStatus
from recidiviz.source_tables.intercom_export_source_tables import (
    INTERCOM_EXPORT_METADATA_DATASET,
    INTERCOM_EXPORT_METADATA_EXPORT_TRACKER_TABLE_ID,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class TestIntercomExportBigQueryTableManager(unittest.TestCase):
    """Tests IntercomExportBigQueryTableManager writing to and reading from table"""

    def setUp(self) -> None:
        self.mock_client = MagicMock(spec=BigQueryClientImpl)
        self.bq_table_manager = IntercomExportBigQueryTableManager(
            client=self.mock_client
        )

        test_export_datetime = datetime.now(timezone.utc)
        self.cloud_run_job_info = IntercomCloudRunJobInfo(
            export_datetime=test_export_datetime,
            export_window_start_inclusive=test_export_datetime - timedelta(days=1),
            export_window_end_inclusive=test_export_datetime,
            status=IntercomCloudRunJobStatus.SUCCESS,
        )

    def test_write_to_table(self) -> None:
        """Test that IntercomExportBigQueryTableManager.write_to_table() runs without errors"""

        mock_job = MagicMock(spec=LoadJob)
        self.mock_client.load_into_table_async.return_value = mock_job

        self.bq_table_manager.write_to_table(self.cloud_run_job_info)
        big_query_address = BigQueryAddress(
            dataset_id=INTERCOM_EXPORT_METADATA_DATASET,
            table_id=INTERCOM_EXPORT_METADATA_EXPORT_TRACKER_TABLE_ID,
        )

        self.mock_client.load_into_table_async.assert_called_once_with(
            address=big_query_address, rows=[self.cloud_run_job_info.to_json()]
        )

    def test_write_to_table_fails(self) -> None:
        """Test that IntercomExportBigQueryTableManager.write_to_table() properly raises GoogleAPICallError"""

        mock_job = MagicMock(spec=LoadJob)
        # GoogleCloudError raises GoogleAPICallError
        mock_job.result.side_effect = exceptions.GoogleCloudError("Load job failed")
        self.mock_client.load_into_table_async.return_value = mock_job

        with self.assertRaises(exceptions.GoogleCloudError) as context:
            self.bq_table_manager.write_to_table(self.cloud_run_job_info)

        self.assertIn("Load job failed", str(context.exception))

    def test_write_to_table_timeout_error(self) -> None:
        """Test that IntercomExportBigQueryTableManager.write_to_table() properly raises TimeoutError"""

        mock_job = MagicMock(spec=LoadJob)
        mock_job.result.side_effect = TimeoutError()
        self.mock_client.load_into_table_async.return_value = mock_job

        with self.assertRaises(TimeoutError):
            self.bq_table_manager.write_to_table(self.cloud_run_job_info)

    def test_get_latest_export_window_end_timeout_error(self) -> None:
        """
        Test that IntercomExportBigQueryTableManager.get_latest_export_window_end()
        properly raises TimeoutError
        """

        mock_job = MagicMock(spec=QueryJob)
        mock_job.result.side_effect = TimeoutError()
        self.mock_client.run_query_async.return_value = mock_job

        with self.assertRaises(TimeoutError):
            self.bq_table_manager.get_latest_export_window_end()


class TestIntercomExportBigQueryTableManagerWithEmulator(BigQueryEmulatorTestCase):
    """
    Emulator-backed tests that run the real query against a live (emulated)
    BigQuery, so query-shape and return-type bugs surface instead of being
    mocked past.
    """

    def setUp(self) -> None:
        super().setUp()
        self.tracker_address = BigQueryAddress(
            dataset_id=INTERCOM_EXPORT_METADATA_DATASET,
            table_id=INTERCOM_EXPORT_METADATA_EXPORT_TRACKER_TABLE_ID,
        )
        self.create_mock_table(
            address=self.tracker_address,
            schema=build_intercom_export_metadata_export_tracker_schema(),
        )
        self.bq_table_manager = IntercomExportBigQueryTableManager(
            client=self.bq_client
        )

    def _row(self, *, window_end: datetime, status: IntercomCloudRunJobStatus) -> dict:
        return {
            EXPORT_DATETIME_COLUMN_NAME: window_end,
            EXPORT_WINDOW_START_INCLUSIVE_COLUMN_NAME: window_end - timedelta(days=1),
            EXPORT_WINDOW_END_INCLUSIVE_COLUMN_NAME: window_end,
            STATUS_COLUMN_NAME: status.value,
        }

    def test_returns_latest_successful_window_end(self) -> None:
        """
        Tests that IntercomExportBigQueryTableManager.get_latest_export_window_end()
        properly returns the latest export_window_end_inclusive datetime from the table.
        """

        self.load_rows_into_table(
            self.tracker_address,
            [
                self._row(
                    window_end=datetime(2026, 7, 1, tzinfo=timezone.utc),
                    status=IntercomCloudRunJobStatus.SUCCESS,
                ),
                self._row(
                    window_end=datetime(2026, 7, 10, tzinfo=timezone.utc),
                    status=IntercomCloudRunJobStatus.SUCCESS,
                ),
                # A later FAILURE row that must be ignored.
                self._row(
                    window_end=datetime(2026, 7, 14, tzinfo=timezone.utc),
                    status=IntercomCloudRunJobStatus.FAILURE,
                ),
            ],
        )

        self.assertEqual(
            datetime(2026, 7, 10, tzinfo=timezone.utc),
            self.bq_table_manager.get_latest_export_window_end(),
        )

    def test_returns_latest_successful_window_end_value_error(self) -> None:
        """
        Tests that IntercomExportBigQueryTableManager.get_latest_export_window_end()
        properly raises ValueError when no successful row is returned from the table.
        """

        self.load_rows_into_table(
            self.tracker_address,
            [
                self._row(
                    window_end=datetime(2026, 7, 1, tzinfo=timezone.utc),
                    status=IntercomCloudRunJobStatus.FAILURE,
                ),
                self._row(
                    window_end=datetime(2026, 7, 10, tzinfo=timezone.utc),
                    status=IntercomCloudRunJobStatus.FAILURE,
                ),
                self._row(
                    window_end=datetime(2026, 7, 14, tzinfo=timezone.utc),
                    status=IntercomCloudRunJobStatus.FAILURE,
                ),
            ],
        )

        with self.assertRaises(ValueError) as context:
            self.bq_table_manager.get_latest_export_window_end()

        self.assertIn(
            "No successful Intercom export job runs found",
            str(context.exception),
        )
