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
"""Tests for the SQL to GCS export process."""
import datetime
import os
import unittest
from unittest.mock import MagicMock

import pytest

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tools.ingest.regions.us_ne.export_sql_to_gcs import (
    UsNeSqlServerConnectionManager,
)
from recidiviz.tools.ingest.regions.us_ne.sql_to_gcs_export_tasks import (
    UsNeSqltoGCSExportTask,
)


class TestProcessUsNeDatabaseExport(unittest.TestCase):
    """Tests for the process_us_ne_database_export function."""

    # there are two issues preventing this test from running locally -
    # the binary form of pymssql is incorrectly linked on ARM on python 3.11 https://github.com/pymssql/pymssql/issues/769
    # the workaround is to build the module locally
    # which is also currently not working https://github.com/pymssql/pymssql/issues/937
    # but there is active development on a fix
    # TODO(#41296) Remove skipif
    @pytest.mark.skipif(
        os.getenv("CI") != "true", reason="ongoing pymssql issue on MAC"
    )
    def test_process_database_export_failure(self) -> None:
        # pylint: disable=import-outside-toplevel
        from recidiviz.tools.ingest.regions.us_ne.export_sql_to_gcs import (
            UsNeGCSFileUploader,
            UsNeSqlTableToRawFileExporter,
            process_us_ne_database_export,
        )

        mock_exporter = MagicMock(spec=UsNeSqlTableToRawFileExporter)
        mock_exporter.dry_run = False
        mock_uploader = MagicMock(spec=UsNeGCSFileUploader)

        export_tasks = [
            UsNeSqltoGCSExportTask.for_qualified_table_name(
                qualified_table_name=f,
                update_datetime=datetime.datetime(2023, 1, 1),
                region_raw_file_config=DirectIngestRegionRawFileConfig(
                    StateCode.US_NE.value
                ),
            )
            for f in ["DCS_WEB.table_1", "DCS_WEB.table_2"]
        ]

        # Simulate an exception during export
        mock_exporter.export_data_to_csv.side_effect = [
            export_tasks[0].file_name,
            Exception("Export failed"),
        ]

        successful_exports, failed_exports = process_us_ne_database_export(
            export_tasks, mock_exporter, mock_uploader
        )

        mock_exporter.open_connection.assert_called_once()
        mock_exporter.begin_snapshot_transaction.assert_called_once()
        mock_exporter.export_data_to_csv.assert_any_call(export_tasks[0])
        mock_exporter.export_data_to_csv.assert_any_call(export_tasks[1])
        # Shouldn't call upload_csv_raw_file for the failed export task
        mock_uploader.upload_csv_raw_file.assert_called_once_with(
            local_file_path=export_tasks[0].file_name,
            gcs_file_name=export_tasks[0].file_name,
        )
        mock_exporter.commit_transaction.assert_called_once()
        mock_exporter.close_connection.assert_called_once()

        self.assertEqual(successful_exports, [export_tasks[0]])
        self.assertEqual(len(failed_exports), 1)
        self.assertEqual(failed_exports[0][0], export_tasks[1])

    @pytest.mark.skipif(
        os.getenv("CI") != "true", reason="ongoing pymssql issue on MAC"
    )
    def test_process_database_export_dry_run(self) -> None:
        # pylint: disable=import-outside-toplevel
        from recidiviz.tools.ingest.regions.us_ne.export_sql_to_gcs import (
            UsNeGCSFileUploader,
            UsNeSqlTableToRawFileExporter,
            process_us_ne_database_export,
        )

        mock_connection_manager = MagicMock(spec=UsNeSqlServerConnectionManager)
        mock_exporter = UsNeSqlTableToRawFileExporter(
            connection_manager=mock_connection_manager, dry_run=True
        )
        mock_gcsfs = MagicMock(spec=GCSFileSystem)
        mock_uploader = UsNeGCSFileUploader(
            gcsfs=mock_gcsfs, destination_bucket=GcsfsBucketPath("test"), dry_run=True
        )

        export_tasks = [
            UsNeSqltoGCSExportTask.for_qualified_table_name(
                qualified_table_name=f,
                update_datetime=datetime.datetime(2023, 1, 1),
                region_raw_file_config=DirectIngestRegionRawFileConfig(
                    StateCode.US_NE.value
                ),
            )
            for f in ["DCS_WEB.table_1", "DCS_WEB.table_2"]
        ]

        successful_exports, failed_exports = process_us_ne_database_export(
            export_tasks, mock_exporter, mock_uploader
        )

        # No actual external operations should be called in dry run mode
        mock_connection_manager.open_connection.assert_not_called()
        mock_connection_manager.begin_snapshot_transaction.assert_not_called()
        mock_connection_manager.execute_query_with_batched_fetch.assert_not_called()
        mock_connection_manager.commit_transaction.assert_not_called()
        mock_connection_manager.close_connection.assert_not_called()

        mock_gcsfs.upload_from_contents_handle_stream.assert_not_called()

        self.assertEqual(successful_exports, export_tasks)
        self.assertEqual(failed_exports, [])
