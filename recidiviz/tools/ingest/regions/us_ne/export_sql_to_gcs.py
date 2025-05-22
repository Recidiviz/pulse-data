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
"""Script for exporting US_NE's SQL Server data to GCS

The script connects to US_NE's SQL Server databases via SSH tunnel. Then for each file tag,
it exports the corresponding SQL Server table to a CSV file according to its raw file config.
The CSV file is then uploaded to a raw files GCS bucket in the recidiviz-us-ne-ingest project,
with an update_datetime of the script's start time.

The bucket is synced with US_NE's ingest bucket in prod and staging through a separate storage transfer job.

Usage:
    python -m recidiviz.tools.ingest.regions.us_ne.export_sql_to_gcs
"""
import csv
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import attr
import pymssql
from sshtunnel import SSHTunnelForwarder

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tools.ingest.regions.us_ne.sql_to_gcs_export_config import (
    UsNeDatabaseConnectionConfigProvider,
)
from recidiviz.tools.ingest.regions.us_ne.sql_to_gcs_export_tasks import (
    UsNeDatabaseName,
    UsNeSqltoGCSExportTask,
    sql_to_gcs_export_tasks_by_db,
)
from recidiviz.utils.log_helpers import log_info_with_fill
from recidiviz.utils.string import StrictStringFormatter

CSV_CONTENT_TYPE = "text/csv"

US_NE_RAW_FILES_BUCKET = "recidiviz-ingest-us-ne-raw-files"
US_NE_INGEST_PROJECT_ID = "recidiviz-ingest-us-ne"

MAX_ROWS_PER_CURSOR_FETCH = 1000


EXPORT_QUERY = "SELECT {columns} FROM {table_name}"
BEGIN_SNAPSHOT_TRANSACTION_QUERY = (
    "BEGIN TRANSACTION; SET TRANSACTION ISOLATION LEVEL SNAPSHOT"
)
COMMIT_TRANSACTION_QUERY = "COMMIT"


@attr.define
class UsNeSqlServerConnectionManager:
    """Handles SQL query execution and connection management to a US_NE SQL server db."""

    db_name: UsNeDatabaseName
    db_connection_config: UsNeDatabaseConnectionConfigProvider
    _db_connection: pymssql.Connection | None = attr.ib(default=None)

    def execute_procedural_query(self, query: str) -> None:
        """Execute a procedural SQL query with no expected results."""
        if not self._db_connection:
            raise ValueError("Database connection is not open.")

        cursor: pymssql.Cursor | None = None
        try:
            cursor = self._db_connection.cursor()
            cursor.execute(query)
        finally:
            if cursor:
                cursor.close()

    def open_connection(self) -> None:
        """Open a connection to the database."""
        if self._db_connection:
            raise ValueError("Database connection is already open.")
        self._db_connection = pymssql.connect(
            read_only=True,
            **self.db_connection_config.get_db_connection_config(db_name=self.db_name),
        )

    def close_connection(self) -> None:
        """Close the database connection."""
        if self._db_connection:
            self._db_connection.close()
            self._db_connection = None

    def begin_snapshot_transaction(self) -> None:
        """Begin a transaction with snapshot isolation level. Snapshot isolation ensures
        that data read by any statement in a transaction is the transactionally consistent
        version of the data that existed at the start of the transaction, without holding locks
        """
        self.execute_procedural_query(BEGIN_SNAPSHOT_TRANSACTION_QUERY)

    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        self.execute_procedural_query(COMMIT_TRANSACTION_QUERY)

    def execute_query_with_batched_fetch(
        self, query: str, batch_size: int = MAX_ROWS_PER_CURSOR_FETCH
    ) -> Iterator[list[Any]]:
        """Execute a SQL query and yield batches of results.

        Args:
            query: The SQL query to execute
            batch_size: Number of rows to fetch in each batch

        Yields:
            The column headers, then batches of query results
        """
        if not self._db_connection:
            raise ValueError("Database connection is not open.")

        cursor: pymssql.Cursor | None = None
        try:
            cursor = self._db_connection.cursor()
            cursor.execute(query)

            # Get column descriptions for the caller
            columns = [desc[0] for desc in cursor.description]
            yield columns

            # Fetch and yield batches of rows
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows
        finally:
            if cursor:
                cursor.close()


@attr.define
class UsNeSqlTableToRawFileExporter:
    """Exports SQL Server tables to raw CSV files."""

    region_raw_file_config: DirectIngestRegionRawFileConfig
    connection_manager: UsNeSqlServerConnectionManager

    @classmethod
    def build(
        cls,
        region_raw_file_config: DirectIngestRegionRawFileConfig,
        db_name: UsNeDatabaseName,
        db_connection_config: UsNeDatabaseConnectionConfigProvider,
    ) -> "UsNeSqlTableToRawFileExporter":
        return cls(
            region_raw_file_config=region_raw_file_config,
            connection_manager=UsNeSqlServerConnectionManager(
                db_name=db_name, db_connection_config=db_connection_config
            ),
        )

    def open_connection(self) -> None:
        """Open a connection to the database"""
        self.connection_manager.open_connection()

    def close_connection(self) -> None:
        """Close the database connection."""
        self.connection_manager.close_connection()

    def begin_snapshot_transaction(self) -> None:
        """Begin a transaction with snapshot isolation level."""
        self.connection_manager.begin_snapshot_transaction()

    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        self.connection_manager.commit_transaction()

    def export_data_to_csv(self, export_task: UsNeSqltoGCSExportTask) -> str:
        """Export data from the specified table to a CSV file.
        Returns the local path of the exported file."""
        raw_file_config = self.region_raw_file_config.raw_file_configs[
            export_task.table_name
        ]

        query = StrictStringFormatter().format(
            EXPORT_QUERY,
            columns=",".join(col.name for col in raw_file_config.current_columns),
            table_name=export_task.table_name,
        )

        # Ensure the directory exists
        output_path = Path(export_task.file_name)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(
            output_path,
            mode="w",
            newline="",
            encoding=raw_file_config.encoding,
        ) as f:
            writer = csv.writer(
                f,
                lineterminator=raw_file_config.line_terminator,
                delimiter=raw_file_config.separator,
                quoting=raw_file_config.quoting_mode,
            )
            # Process the results in batches
            result_generator = self.connection_manager.execute_query_with_batched_fetch(
                query
            )

            # First row of the exported CSV will always contain column headers
            columns = next(result_generator, None)
            if not columns:
                raise ValueError(
                    f"Found no columns in the exported file for file [{raw_file_config.file_tag}]. Expected every file to at least have a columns row."
                )
            # TODO(#41296): Also add a unittest that prevents this flag from being set for NE
            if raw_file_config.infer_columns_from_config:
                raise ValueError(
                    "Did not expect any files with infer_columns_from_config=True in NE"
                )
            writer.writerow(columns)

            for rows in result_generator:
                writer.writerows(rows)

        return str(output_path)


@attr.define
class UsNeGCSFileUploader:
    """Manages uploading local csv raw data files to GCS."""

    gcsfs: GCSFileSystem
    destination_bucket: GcsfsBucketPath

    def upload_csv_raw_file(self, local_file_path: str, gcs_file_name: str) -> None:
        """Upload a local csv raw data file to GCS with normalized raw file name format."""
        self.gcsfs.upload_from_contents_handle_stream(
            path=GcsfsFilePath.from_directory_and_file_name(
                self.destination_bucket,
                gcs_file_name,
            ),
            contents_handle=LocalFileContentsHandle(
                local_file_path=local_file_path, cleanup_file=True
            ),
            content_type=CSV_CONTENT_TYPE,
        )


@contextmanager
def exporter_connection(exporter: UsNeSqlTableToRawFileExporter) -> Iterator[None]:
    """Manage the connection to the database for the exporter in order to ensure
    that the connection is opened and closed properly.
    """
    exporter.open_connection()
    try:
        yield
    finally:
        exporter.close_connection()


def process_us_ne_database_export(
    export_tasks: list[UsNeSqltoGCSExportTask],
    exporter: UsNeSqlTableToRawFileExporter,
    uploader: UsNeGCSFileUploader,
) -> tuple[
    list[UsNeSqltoGCSExportTask], list[tuple[UsNeSqltoGCSExportTask, Exception]]
]:
    """Process exports for a single database.

    1. Open a connection to the database
    2. Begin transaction with snapshot isolation level
    3. For each task, export data to CSV and upload to GCS sequentially
    4. Commit the transaction
    5. Close the connection

    We need to export each table then upload to GCS sequentially (files are deleted after upload),
    rather than exporting all tables then uploading to GCS in parallel, in order to avoid
    avoid running out of memory in the Cloud Run job, which have a memory limit of 32G.
    """
    successful_exports: list[UsNeSqltoGCSExportTask] = []
    failed_exports: list[tuple[UsNeSqltoGCSExportTask, Exception]] = []

    def process_export_task(export_task: UsNeSqltoGCSExportTask) -> None:
        # TODO(#41296): Add retry logic for transient errors
        try:
            logging.info(
                "Exporting table [%s]",
                export_task.table_name,
            )
            exported_data_local_path = exporter.export_data_to_csv(export_task)

            logging.info(
                "Uploading [%s] to GCS at [%s]",
                exported_data_local_path,
                export_task.file_name,
            )
            uploader.upload_csv_raw_file(
                local_file_path=exported_data_local_path,
                gcs_file_name=export_task.file_name,
            )

            successful_exports.append(export_task)
            logging.info("Successfully processed [%s]", export_task.to_str())
        except Exception as e:
            logging.error(
                "Failed to process [%s]: %s",
                export_task.to_str(),
                str(e),
                exc_info=True,
            )
            failed_exports.append((export_task, e))

    with exporter_connection(exporter):
        try:
            exporter.begin_snapshot_transaction()
            for export_task in export_tasks:
                process_export_task(export_task)
        finally:
            exporter.commit_transaction()

    return successful_exports, failed_exports


def export_us_ne_sql_tables_to_gcs(
    destination_bucket_name: str,
    db_connection_config: UsNeDatabaseConnectionConfigProvider,
    update_datetime: datetime.datetime,
) -> None:
    """For each US_NE file tag, export the SQL server table to a CSV file according to
    its raw file config, then upload it to a raw files GCS bucket in the recidiviz-us-ne-ingest project.
    This bucket synced with US_NE's ingest bucket in prod and staging through a storage transfer job.
    """
    region_raw_file_config = DirectIngestRegionRawFileConfig(
        region_code=StateCode.US_NE.value,
    )
    uploader = UsNeGCSFileUploader(
        gcsfs=GcsfsFactory.build(),
        destination_bucket=GcsfsBucketPath(destination_bucket_name),
    )

    # TODO(#41296) Make table list configurable
    export_tasks_by_db: dict[
        UsNeDatabaseName, list[UsNeSqltoGCSExportTask]
    ] = sql_to_gcs_export_tasks_by_db(
        list(region_raw_file_config.raw_file_tags), update_datetime
    )

    with SSHTunnelForwarder(**db_connection_config.ssh_tunnel_config):
        all_successful_exports: list[UsNeSqltoGCSExportTask] = []
        all_failed_exports: list[tuple[UsNeSqltoGCSExportTask, Exception]] = []

        # We need to export all tables in a database in a single thread
        # because pymssql can only have one active cursor per connection
        # and we need to export all tables in a single transaction
        with ThreadPoolExecutor(max_workers=len(export_tasks_by_db)) as executor:
            future_to_db = {
                executor.submit(
                    process_us_ne_database_export,
                    export_tasks=tasks,
                    exporter=UsNeSqlTableToRawFileExporter.build(
                        region_raw_file_config, db_name, db_connection_config
                    ),
                    uploader=uploader,
                )
                for db_name, tasks in export_tasks_by_db.items()
            }

            for future in as_completed(future_to_db):
                successful_exports, failed_exports = future.result()
                all_successful_exports.extend(successful_exports)
                all_failed_exports.extend(failed_exports)

    log_info_with_fill(s="")
    log_info_with_fill(s="  RESULTS  ")
    log_info_with_fill(s="")
    if all_successful_exports:
        logging.info(
            "Successfully exported [%s] tables: \n %s",
            len(all_successful_exports),
            "\n".join(f"\t- {export.to_str()}" for export in all_successful_exports),
        )
        log_info_with_fill(s="")
    if all_failed_exports:
        raise RuntimeError(
            "Exports failed with the following messages: ", all_failed_exports
        )


def main() -> None:
    """Main entry point for the script."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    export_us_ne_sql_tables_to_gcs(
        destination_bucket_name=US_NE_RAW_FILES_BUCKET,
        db_connection_config=UsNeDatabaseConnectionConfigProvider(
            US_NE_INGEST_PROJECT_ID
        ),
        update_datetime=datetime.datetime.now(tz=datetime.UTC),
    )
    logging.info("Export completed successfully!")


if __name__ == "__main__":
    main()
