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
import tempfile
import unittest
from unittest.mock import MagicMock, PropertyMock, patch

import pymssql

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tools.ingest.regions.us_ne.export_sql_to_gcs import (
    UsNeGCSFileUploader,
    UsNeSqlServerConnectionManager,
    UsNeSqlTableToRawFileExporter,
    process_us_ne_database_export,
)
from recidiviz.tools.ingest.regions.us_ne.sql_to_gcs_export_config import (
    UsNeDatabaseConnectionConfigProvider,
)
from recidiviz.tools.ingest.regions.us_ne.sql_to_gcs_export_tasks import (
    UsNeDatabaseName,
    UsNeSqltoGCSExportTask,
)


class TestProcessUsNeDatabaseExport(unittest.TestCase):
    """Tests for the process_us_ne_database_export function."""

    def test_process_database_export_failure(self) -> None:
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
        mock_exporter.begin_transaction.assert_called_once()
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

    def test_process_database_export_dry_run(self) -> None:
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
        mock_connection_manager.begin_transaction.assert_not_called()
        mock_connection_manager.execute_query_with_batched_fetch.assert_not_called()
        mock_connection_manager.commit_transaction.assert_not_called()
        mock_connection_manager.close_connection.assert_not_called()

        mock_gcsfs.upload_from_contents_handle_stream.assert_not_called()

        self.assertEqual(successful_exports, export_tasks)
        self.assertEqual(failed_exports, [])

    def test_process_database_export_retry_on_open_connection_error(self) -> None:
        mock_exporter = MagicMock(spec=UsNeSqlTableToRawFileExporter)
        mock_exporter.dry_run = False
        mock_uploader = MagicMock(spec=UsNeGCSFileUploader)

        export_tasks = [
            UsNeSqltoGCSExportTask.for_qualified_table_name(
                qualified_table_name="DCS_WEB.table_1",
                update_datetime=datetime.datetime(2023, 1, 1),
                region_raw_file_config=DirectIngestRegionRawFileConfig(
                    StateCode.US_NE.value
                ),
            )
        ]

        open_call_count = 0

        def side_effect_open_connection() -> None:
            nonlocal open_call_count
            open_call_count += 1
            if open_call_count == 1:
                raise pymssql.OperationalError("Connection refused: could not open")

        mock_exporter.open_connection = MagicMock(
            side_effect=side_effect_open_connection
        )
        mock_exporter.export_data_to_csv.return_value = export_tasks[0].file_name

        with patch("time.sleep") as _mock_sleep:
            successful_exports, failed_exports = process_us_ne_database_export(
                export_tasks, mock_exporter, mock_uploader
            )

        self.assertEqual(len(successful_exports), 1)
        self.assertEqual(successful_exports[0], export_tasks[0])
        self.assertEqual(len(failed_exports), 0)

        # Verify open_connection was retried
        self.assertEqual(mock_exporter.open_connection.call_count, 2)

        mock_exporter.begin_transaction.assert_called_once()
        mock_exporter.export_data_to_csv.assert_called_once_with(export_tasks[0])
        mock_uploader.upload_csv_raw_file.assert_called_once()
        mock_exporter.commit_transaction.assert_called_once()
        mock_exporter.close_connection.assert_called_once()


class TestUsNeSqlServerConnectionManager(unittest.TestCase):
    """Tests for the UsNeSqlServerConnectionManager."""

    def test_execute_query_with_batched_fetch(self) -> None:
        mock_connection = MagicMock(spec=pymssql.Connection)
        mock_cursor = MagicMock()
        mock_cursor.description = [["column1"], ["column2"]]
        mock_cursor.fetchmany.side_effect = [
            [
                (1, "data1"),
                (2, "data2"),
            ],
            [],
        ]
        mock_connection.cursor.return_value = mock_cursor
        manager = UsNeSqlServerConnectionManager(
            db_name=UsNeDatabaseName.DCS_WEB,
            db_connection=mock_connection,
            db_connection_config=UsNeDatabaseConnectionConfigProvider(
                project_id="test_project"
            ),
        )

        result_generator = manager.execute_query_with_batched_fetch(
            query="SELECT * FROM test_table",
            batch_size=2,
        )
        columns = next(result_generator, None)
        self.assertEqual(columns, ["column1", "column2"])

        for rows in result_generator:
            self.assertEqual(rows, [(1, "data1"), (2, "data2")])


class TestUsNeSqlTableToRawFileExporter(unittest.TestCase):
    """Tests for the UsNeSqlTableToRawFileExporter."""

    def test_export_data_to_csv(self) -> None:
        mock_connection_manager = MagicMock(spec=UsNeSqlServerConnectionManager)
        mock_connection_manager.execute_query_with_batched_fetch.return_value = iter(
            [["column1", "column2"], [(1, "data1"), (2, "data2")]]
        )
        exporter = UsNeSqlTableToRawFileExporter(
            connection_manager=mock_connection_manager, dry_run=False
        )

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "recidiviz.tools.ingest.regions.us_ne.sql_to_gcs_export_tasks.UsNeSqltoGCSExportTask.file_name",
            new_callable=PropertyMock,
        ) as mock_file_name:
            mock_file_name.return_value = f"{temp_dir}/test_table.csv"
            export_task = UsNeSqltoGCSExportTask.for_qualified_table_name(
                qualified_table_name="DCS_WEB.test_table",
                update_datetime=datetime.datetime(2023, 1, 1),
                region_raw_file_config=DirectIngestRegionRawFileConfig(
                    StateCode.US_NE.value
                ),
            )
            mock_connection_manager.execute_query_with_batched_fetch.return_value = (
                iter([["column1", "column2"], [(1, "data1"), (2, "data2")]])
            )

            file_name = exporter.export_data_to_csv(export_task)

            # assert file was created and contains expected data
            with open(file_name, "r", encoding="windows-1252") as f:
                content = f.read()
                self.assertEqual("column1‡column2†1‡data1†2‡data2†", content)

    def test_export_data_to_csv_retry_success(self) -> None:
        mock_connection_manager = MagicMock(spec=UsNeSqlServerConnectionManager)
        mock_connection_manager.execute_query_with_batched_fetch.side_effect = [
            pymssql.OperationalError("Temporary failure"),
            iter([["column1", "column2"], [(1, "data1"), (2, "data2")]]),
        ]
        exporter = UsNeSqlTableToRawFileExporter(
            connection_manager=mock_connection_manager, dry_run=False
        )

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "recidiviz.tools.ingest.regions.us_ne.sql_to_gcs_export_tasks.UsNeSqltoGCSExportTask.file_name",
            new_callable=PropertyMock,
        ) as mock_file_name:
            mock_file_name.return_value = f"{temp_dir}/test_table.csv"
            export_task = UsNeSqltoGCSExportTask.for_qualified_table_name(
                qualified_table_name="DCS_WEB.test_table",
                update_datetime=datetime.datetime(2023, 1, 1),
                region_raw_file_config=DirectIngestRegionRawFileConfig(
                    StateCode.US_NE.value
                ),
            )
            mock_connection_manager.execute_query_with_batched_fetch.return_value = (
                iter([["column1", "column2"], [(1, "data1"), (2, "data2")]])
            )

            file_name = exporter.export_data_to_csv(export_task)

            # assert file was created and contains expected data
            with open(file_name, "r", encoding="windows-1252") as f:
                content = f.read()
                self.assertEqual("column1‡column2†1‡data1†2‡data2†", content)
