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
from google.cloud.bigquery import LoadJob

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.query.state.dataset_config import INTERCOM_EXPORT_DATASET
from recidiviz.intercom.intercom_export_bq_table_manager import (
    IntercomExportBigQueryTableManager,
)
from recidiviz.intercom.types import IntercomCloudRunJobInfo, IntercomCloudRunJobStatus
from recidiviz.source_tables.intercom_exports_source_table import (
    INTERCOM_EXPORT_TRACKER_TABLE_ID,
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
            export_window_start_inclusive=test_export_datetime,
            export_window_end_inclusive=test_export_datetime - timedelta(hours=1),
            status=IntercomCloudRunJobStatus.SUCCESS,
        )

    def test_write_to_table(self) -> None:
        """Test that IntercomExportBigQueryTableManager.write_to_table() runs without errors"""

        mock_job = MagicMock(spec=LoadJob)
        self.mock_client.load_into_table_async.return_value = mock_job

        self.bq_table_manager.write_to_table(self.cloud_run_job_info)
        big_query_address = BigQueryAddress(
            dataset_id=INTERCOM_EXPORT_DATASET,
            table_id=INTERCOM_EXPORT_TRACKER_TABLE_ID,
        )

        self.mock_client.load_into_table_async.assert_called_once_with(
            address=big_query_address, rows=[self.cloud_run_job_info.to_json()]
        )

    def test_write_to_table_fails(self) -> None:
        """Test that IntercomExportBigQueryTableManager.write_to_table() properly raises GoogleAPICallError"""

        load_job = self.mock_client.load_into_table_async.return_value = MagicMock(
            spec=LoadJob
        )
        # GoogleCloudError raises GoogleAPICallError
        load_job.result.side_effect = exceptions.GoogleCloudError("Load job failed")

        with self.assertRaises(exceptions.GoogleCloudError) as context:
            self.bq_table_manager.write_to_table(self.cloud_run_job_info)

        self.assertIn("Load job failed", str(context.exception))

    def test_write_to_table_timeout_error(self) -> None:
        """Test that IntercomExportBigQueryTableManager.write_to_table() properly raises TimeoutError"""

        load_job = self.mock_client.load_into_table_async.return_value = MagicMock(
            spec=LoadJob
        )
        load_job.result.side_effect = TimeoutError(
            "Load job did not complete within 60 seconds"
        )

        with self.assertRaises(TimeoutError):
            self.bq_table_manager.write_to_table(self.cloud_run_job_info)
