# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Airflow tasks for the clean up and storage step of the raw data import dag"""
import logging
from concurrent import futures
from typing import List

from airflow.decorators import task

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_storage_directory_path_for_state,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

MAX_DELETE_THREADS = 16
MAX_RENAME_THREADS = 16


@task
def clean_up_temporary_files(serialized_temporary_paths: List[str]) -> None:
    """Deletes all paths in |serialized_temporary_paths|, if they exist."""
    temporary_paths = [
        GcsfsFilePath.from_absolute_path(path) for path in serialized_temporary_paths
    ]

    fs = GcsfsFactory.build()

    with futures.ThreadPoolExecutor(max_workers=MAX_DELETE_THREADS) as executor:
        delete_futures = [executor.submit(fs.delete, path) for path in temporary_paths]

        failed: List[Exception] = []
        deleted = 0
        for f in futures.as_completed(delete_futures):
            try:
                f.result()
                deleted += 1
            except Exception as e:
                failed.append(e)

    logging.info(
        "Confirmed [%s/%s] paths are deleted",
        deleted,
        len(temporary_paths),
    )

    if failed:
        raise ExceptionGroup("Errors occurred during path deletion", failed)


@task
def clean_up_temporary_tables(serialized_temporary_tables: List[str]) -> None:
    """Concurrently deletes |serialized_temporary_tables| from BigQuery, if they exist."""
    temporary_tables = [
        BigQueryAddress.from_str(serialized_temporary_table)
        for serialized_temporary_table in serialized_temporary_tables
    ]

    bq_client = BigQueryClientImpl()

    with futures.ThreadPoolExecutor(max_workers=MAX_DELETE_THREADS) as executor:
        delete_futures = [
            executor.submit(bq_client.delete_table, table_address, not_found_ok=True)
            for table_address in temporary_tables
        ]

        failed: List[Exception] = []
        completed: int = 0

        for f in futures.as_completed(delete_futures):
            try:
                f.result()
                completed += 1
            except Exception as e:
                failed.append(e)

    logging.info(
        "Confirmed [%s/%s] tables are deleted", completed, len(temporary_tables)
    )

    if failed:
        raise ExceptionGroup("Errors occurred during table deletion", failed)


@task
def move_successfully_imported_paths_to_storage(
    region_code: str,
    raw_data_instance: DirectIngestInstance,
    serialized_successfully_imported_paths: List[str],
) -> None:
    """Concurrently renames |serialized_successfully_imported_paths| from their
    unprocessed to processed name in storage.
    """
    successfully_imported_paths = [
        GcsfsFilePath.from_absolute_path(path)
        for path in serialized_successfully_imported_paths
    ]

    fs = DirectIngestGCSFileSystem(GcsfsFactory.build())

    storage_path = gcsfs_direct_ingest_storage_directory_path_for_state(
        region_code=region_code, ingest_instance=raw_data_instance
    )

    with futures.ThreadPoolExecutor(max_workers=MAX_RENAME_THREADS) as executor:
        delete_futures = [
            executor.submit(
                fs.mv_unprocessed_path_to_processed_path_in_storage, path, storage_path
            )
            for path in successfully_imported_paths
        ]

        num_successfully_moved_paths = 0
        failed_moves: List[Exception] = []

        for f in futures.as_completed(delete_futures):
            try:
                _ = f.result()
                num_successfully_moved_paths += 1
            except Exception as e:
                failed_moves.append(e)

    logging.info(
        "Moved [%s/[%s] files to their processed path in storage",
        num_successfully_moved_paths,
        len(successfully_imported_paths),
    )

    if failed_moves:
        raise ExceptionGroup(
            "Errors occurred moving files to their processed paths in storage",
            failed_moves,
        )
