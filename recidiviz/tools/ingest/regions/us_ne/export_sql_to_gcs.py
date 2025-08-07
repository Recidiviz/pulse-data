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

If you want to export a table that does not have a raw file config, you can run the script ad hoc and provide
the qualified table name as a command line argument. The table name should be in the format {database}.{table},
and it will be exported using the default raw file config for the state. If you want the table to become a
part of the regular export process, you need to add a raw file config for it.

Usage:
    python -m recidiviz.tools.ingest.regions.us_ne.export_sql_to_gcs [--destination-bucket raw-files-bucket] \
        [--dry-run True] [--qualified-table-names DCS_WEB.table1,DCS_MVS.table2]

"""
import argparse
import datetime
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any, Iterator

import attr
import pymssql
from sshtunnel import SSHTunnelForwarder

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common import attr_validators
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
)
from recidiviz.utils.list_helpers import group_by
from recidiviz.utils.log_helpers import log_info_with_fill
from recidiviz.utils.params import str_to_bool, str_to_list
from recidiviz.utils.string import StrictStringFormatter

CSV_CONTENT_TYPE = "text/csv"

US_NE_RAW_FILES_BUCKET = "recidiviz-ingest-us-ne-raw-files"
US_NE_INGEST_PROJECT_ID = "recidiviz-ingest-us-ne"

MAX_ROWS_PER_CURSOR_FETCH = 1000
MAX_RETRIES = 3

EXPORT_QUERY = "SELECT {columns} FROM {table_name}"
BEGIN_TRANSACTION_QUERY = "BEGIN TRANSACTION"
COMMIT_TRANSACTION_QUERY = "COMMIT"


@attr.define
class UsNeSqlServerConnectionManager:
    """Handles SQL query execution and connection management to a US_NE SQL server db."""

    db_name: UsNeDatabaseName = attr.ib(
        validator=attr.validators.instance_of(UsNeDatabaseName)
    )
    db_connection_config: UsNeDatabaseConnectionConfigProvider = attr.ib(
        validator=attr.validators.instance_of(UsNeDatabaseConnectionConfigProvider)
    )
    _db_connection: pymssql.Connection | None = attr.ib(
        default=None, validator=attr_validators.is_opt(pymssql.Connection)
    )

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

    def begin_transaction(self) -> None:
        """Begin a transaction. Ideally we would use snapshot isolation level,
        but this isn't supported by all of NE's databases. Instead, we use the
        default isolation level of the database, which is typically READ COMMITTED
        """
        self.execute_procedural_query(BEGIN_TRANSACTION_QUERY)

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

    connection_manager: UsNeSqlServerConnectionManager = attr.ib(
        validator=attr.validators.instance_of(UsNeSqlServerConnectionManager)
    )
    dry_run: bool = attr.ib(validator=attr_validators.is_bool)

    @classmethod
    def build(
        cls,
        *,
        db_name: UsNeDatabaseName,
        db_connection_config: UsNeDatabaseConnectionConfigProvider,
        dry_run: bool,
    ) -> "UsNeSqlTableToRawFileExporter":
        return cls(
            connection_manager=UsNeSqlServerConnectionManager(
                db_name=db_name,
                db_connection_config=db_connection_config,
            ),
            dry_run=dry_run,
        )

    def open_connection(self) -> None:
        """Open a connection to the database"""
        if self.dry_run:
            logging.info("Dry run mode: skipping opening database connection.")
            return
        self.connection_manager.open_connection()

    def close_connection(self) -> None:
        """Close the database connection."""
        if self.dry_run:
            logging.info("Dry run mode: skipping closing database connection.")
            return
        self.connection_manager.close_connection()

    def begin_transaction(self) -> None:
        """Begin a transaction."""
        if self.dry_run:
            logging.info("Dry run mode: skipping beginning transaction.")
            return
        self.connection_manager.begin_transaction()

    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if self.dry_run:
            logging.info("Dry run mode: skipping committing transaction.")
            return
        self.connection_manager.commit_transaction()

    def export_data_to_csv(self, export_task: UsNeSqltoGCSExportTask) -> str:
        """Export data from the specified table to a CSV file.
        Returns the local path of the exported file."""
        output_path = Path(export_task.file_name)
        if self.dry_run:
            return str(output_path)

        # Ensure the directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        query = StrictStringFormatter().format(
            EXPORT_QUERY,
            columns=",".join(export_task.columns),
            table_name=export_task.table_name,
        )

        def write_csv_file() -> None:
            """Writes the query results to a CSV file."""
            with open(
                output_path,
                mode="w",
                newline="",
                encoding=export_task.encoding,
                errors="replace",
            ) as f:
                # Process the results in batches
                result_generator = (
                    self.connection_manager.execute_query_with_batched_fetch(query)
                )

                # First row contains column headers
                columns = next(result_generator, None)
                if not columns:
                    raise ValueError(
                        f"Found no columns in the exported file for file [{export_task.file_tag}]. "
                        "Expected every file to at least have a columns row."
                    )

                def format_field(field: Any) -> str:
                    """Format fields to match previous CSV export behavior."""
                    if field is None:
                        return ""
                    if isinstance(field, bool):
                        return "1" if field else "0"
                    if isinstance(field, datetime.datetime):
                        # Format with three-digit millisecond precision
                        return field.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    return str(field)

                def write_csv_row(row: list[Any]) -> None:
                    """Write a single row to the CSV file."""
                    f.write(
                        export_task.delimiter.join(format_field(field) for field in row)
                        + export_task.line_terminator
                    )

                write_csv_row(columns)

                for rows in result_generator:
                    for row in rows:
                        write_csv_row(row)

        retries = 0
        while retries < MAX_RETRIES:
            try:
                write_csv_file()
                break
            except pymssql.OperationalError as e:
                logging.error("OperationalError on attempt %d: %s", retries + 1, e)
                retries += 1

                if retries >= MAX_RETRIES:
                    raise

                # Clear the file if it exists (will be recreated on next attempt)
                if os.path.exists(output_path):
                    try:
                        os.remove(output_path)
                    except OSError as remove_error:
                        logging.warning(
                            "Failed to remove partial file %s: %s",
                            output_path,
                            remove_error,
                        )
                time.sleep(2**retries)

        return str(output_path)


@attr.define
class UsNeGCSFileUploader:
    """Manages uploading local csv raw data files to GCS."""

    gcsfs: GCSFileSystem
    destination_bucket: GcsfsBucketPath = attr.ib(
        validator=attr.validators.instance_of(GcsfsBucketPath)
    )
    dry_run: bool = attr.ib(validator=attr_validators.is_bool)

    def upload_csv_raw_file(self, local_file_path: str, gcs_file_name: str) -> None:
        """Upload a local csv raw data file to GCS with normalized raw file name format."""
        if self.dry_run:
            return
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
    if exporter.dry_run:
        yield
        return

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
    2. Begin transaction
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
            exporter.begin_transaction()
            for export_task in export_tasks:
                process_export_task(export_task)
        finally:
            exporter.commit_transaction()

    return successful_exports, failed_exports


def export_us_ne_sql_tables_to_gcs(
    destination_bucket_name: str,
    db_connection_config: UsNeDatabaseConnectionConfigProvider,
    export_tasks_by_db: dict[UsNeDatabaseName, list[UsNeSqltoGCSExportTask]],
    dry_run: bool,
) -> None:
    """For each US_NE file tag, export the SQL server table to a CSV file, then upload
    it to a raw files GCS bucket in the recidiviz-us-ne-ingest project. This bucket is
    synced with US_NE's ingest bucket in prod and staging through a storage transfer job.
    """

    uploader = UsNeGCSFileUploader(
        gcsfs=GcsfsFactory.build(),
        destination_bucket=GcsfsBucketPath(destination_bucket_name),
        dry_run=dry_run,
    )

    ssh_tunnel = (
        SSHTunnelForwarder(**db_connection_config.ssh_tunnel_config)
        if not dry_run
        else nullcontext()
    )

    with ssh_tunnel:
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
                        db_name=db_name,
                        db_connection_config=db_connection_config,
                        dry_run=dry_run,
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


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Export SQL tables to GCS")
    parser.add_argument(
        "--destination-bucket",
        type=str,
        default=US_NE_RAW_FILES_BUCKET,
        help="GCS bucket name for the raw files",
    )
    parser.add_argument(
        "--qualified-table-names",
        type=str_to_list,
        default=None,
        help="Comma separated list of qualified table names ({database}.{table}) to export."
        "Tables do not need to have a raw file config in order to be exported."
        "If not provided, tables for all file tags for which we do have config will be exported.",
    )
    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs script in dry-run mode, only prints the operations it would perform.",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point for the script."""
    args = parse_args()

    log_prefix = "[DRY RUN] " if args.dry_run else ""
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s - %(levelname)s - {log_prefix}%(message)s",
    )

    region_raw_file_config = DirectIngestRegionRawFileConfig(
        region_code=StateCode.US_NE.value,
    )
    update_datetime = datetime.datetime.now(tz=datetime.UTC)
    if args.qualified_table_names:
        export_tasks = [
            UsNeSqltoGCSExportTask.for_qualified_table_name(
                table, update_datetime, region_raw_file_config
            )
            for table in args.qualified_table_names
        ]
    else:
        export_tasks = [
            UsNeSqltoGCSExportTask.for_file_tag(
                file_tag=file_tag,
                update_datetime=update_datetime,
                raw_file_config=region_raw_file_config.raw_file_configs[file_tag],
            )
            for file_tag, config in region_raw_file_config.raw_file_configs.items()
            if not config.is_recidiviz_generated
        ]

    logging.info(
        "Exporting tables for file tags: %s", [task.file_tag for task in export_tasks]
    )
    export_us_ne_sql_tables_to_gcs(
        destination_bucket_name=args.destination_bucket,
        db_connection_config=UsNeDatabaseConnectionConfigProvider(
            US_NE_INGEST_PROJECT_ID
        ),
        export_tasks_by_db=group_by(items=export_tasks, key_fn=lambda x: x.db),
        dry_run=args.dry_run,
    )
    logging.info("Export completed successfully!")


if __name__ == "__main__":
    main()
